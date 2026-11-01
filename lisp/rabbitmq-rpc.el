;;; rabbitmq-rpc.el --- JSON-RPC transport to rabbitmq-bridge  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Spawns the `rabbitmq-bridge' Rust executable and talks JSON-RPC 2.0
;; over its stdio using `jsonrpc.el'.  Every call is asynchronous: the UI
;; never waits on the broker.  Notifications from the bridge are routed to
;; handlers registered with `rabbitmq-rpc-on'.
;;
;; Errors from the bridge carry a structured `data' payload (see
;; docs/protocol/RPC.md).  `rabbitmq-rpc-call' passes it to the error
;; callback as a plist with :code (a string such as "confirmation_required"),
;; :message, :hint, :ambiguous, :retryable and :detail.

;;; Code:

(require 'jsonrpc)
(require 'cl-lib)
(require 'subr-x)

(defgroup rabbitmq nil
  "RabbitMQ development and administration environment."
  :group 'tools
  :prefix "rabbitmq-")

(defcustom rabbitmq-bridge-program nil
  "Path to the `rabbitmq-bridge' executable.
When nil, look for it on `exec-path', then in the package's
`target/release' and `target/debug' directories."
  :type '(choice (const :tag "Auto-detect" nil) file)
  :group 'rabbitmq)

(defcustom rabbitmq-bridge-arguments nil
  "Extra command-line arguments for the bridge.
For example (\"--audit-log\" \"~/.rabbitmq-el-audit.jsonl\")."
  :type '(repeat string)
  :group 'rabbitmq)

(defcustom rabbitmq-rpc-timeout 30
  "Seconds before an RPC request is considered timed out locally.
A timeout does not mean the operation failed on the broker; interventions
that time out are reported as uncertain."
  :type 'integer
  :group 'rabbitmq)

(defcustom rabbitmq-events-log-size 0
  "Legacy JSON-RPC log size, retained for configuration compatibility.
Traffic logging is always disabled because requests contain credentials."
  :type 'integer
  :group 'rabbitmq)

(defvar rabbitmq-rpc--connection nil
  "The live `jsonrpc-process-connection' to the bridge, or nil.")

(defvar rabbitmq-rpc--handlers (make-hash-table :test #'equal)
  "Map notification method name (string) to a list of handler functions.")

(defvar rabbitmq-rpc--hello nil
  "Result of `bridge.hello' for the current bridge process.")

(defconst rabbitmq-rpc-protocol-version 1
  "Bridge protocol version this frontend speaks.")

(defun rabbitmq-rpc--package-root ()
  "Directory containing this package's sources (the repository root)."
  (let ((here (or load-file-name (locate-library "rabbitmq-rpc") buffer-file-name)))
    (when here
      (file-name-directory (directory-file-name (file-name-directory (file-truename here)))))))

(defun rabbitmq-rpc-find-bridge ()
  "Return the path of the bridge executable, or signal a user error."
  (or (and rabbitmq-bridge-program (executable-find (expand-file-name rabbitmq-bridge-program)))
      (executable-find "rabbitmq-bridge")
      (when-let* ((root (rabbitmq-rpc--package-root)))
        (cl-loop for dir in '("target/release" "target/debug")
                 for f = (expand-file-name (concat dir "/rabbitmq-bridge") root)
                 when (file-executable-p f) return f))
      (user-error "Cannot find rabbitmq-bridge; build it with `cargo build --release' or set `rabbitmq-bridge-program'")))

(defun rabbitmq-rpc-live-p ()
  "Non-nil when the bridge process is running."
  (and rabbitmq-rpc--connection (jsonrpc-running-p rabbitmq-rpc--connection)))

(defun rabbitmq-rpc--dispatch-notification (_conn method params)
  "Route notification METHOD with PARAMS to registered handlers."
  (let ((name (if (symbolp method) (symbol-name method) method)))
    (dolist (fn (gethash name rabbitmq-rpc--handlers))
      (condition-case err
          (funcall fn params)
        (error (message "rabbitmq: handler for %s failed: %S" name err))))
    (dolist (fn (gethash "*" rabbitmq-rpc--handlers))
      (ignore-errors (funcall fn name params)))))

(defun rabbitmq-rpc-on (method fn)
  "Call FN with the params of every notification named METHOD.
METHOD \"*\" receives every notification as (FN NAME PARAMS)."
  (cl-pushnew fn (gethash method rabbitmq-rpc--handlers) :test #'eq))

(defun rabbitmq-rpc-off (method fn)
  "Remove FN from METHOD's notification handlers."
  (puthash method (delq fn (gethash method rabbitmq-rpc--handlers)) rabbitmq-rpc--handlers))

(defvar rabbitmq-rpc-shutdown-hook nil
  "Hook run when the bridge process exits, expectedly or not.")

(defun rabbitmq-rpc--on-shutdown (_conn)
  "Bridge process ended."
  (setq rabbitmq-rpc--connection nil
        rabbitmq-rpc--hello nil)
  (run-hooks 'rabbitmq-rpc-shutdown-hook))

(defun rabbitmq-rpc--no-logging-initargs ()
  "Return `jsonrpc-connection' initargs that disable traffic logging.
jsonrpc 1.0.24+ (Emacs 30+) uses `:events-buffer-config'.  Older
releases, including the one bundled with Emacs 29, only accept
`:events-buffer-scrollback-size'."
  (if (slot-exists-p 'jsonrpc-connection '-events-buffer-config)
      (list :events-buffer-config '(:size 0))
    (list :events-buffer-scrollback-size 0)))

(defun rabbitmq-rpc-start ()
  "Start the bridge if needed and return the connection."
  (unless (rabbitmq-rpc-live-p)
    (let* ((program (rabbitmq-rpc-find-bridge))
           (conn (apply
                  #'make-instance
                  'jsonrpc-process-connection
                  :name "rabbitmq-bridge"
                  (append
                   (rabbitmq-rpc--no-logging-initargs)
                   (list
                  :notification-dispatcher #'rabbitmq-rpc--dispatch-notification
                  :request-dispatcher #'ignore
                  :on-shutdown #'rabbitmq-rpc--on-shutdown
                  :process (lambda ()
                             (make-process
                              :name "rabbitmq-bridge"
                              :command (cons program rabbitmq-bridge-arguments)
                              :connection-type 'pipe
                              :coding 'utf-8-emacs-unix
                              :noquery t
                              :stderr (get-buffer-create "*rabbitmq-bridge stderr*"))))))))
      (setq rabbitmq-rpc--connection conn)
      (let ((hello (jsonrpc-request conn 'bridge.hello (make-hash-table) :timeout 10)))
        (setq rabbitmq-rpc--hello hello)
        (let ((v (plist-get hello :protocol_version)))
          (unless (eql v rabbitmq-rpc-protocol-version)
            (display-warning 'rabbitmq
                             (format "Bridge speaks protocol %s, frontend expects %s; upgrade the bridge or the package"
                                     v rabbitmq-rpc-protocol-version)))))))
  rabbitmq-rpc--connection)

(defun rabbitmq-rpc-stop ()
  "Ask the bridge to close all sessions and exit."
  (interactive)
  (when (rabbitmq-rpc-live-p)
    (let ((conn rabbitmq-rpc--connection))
      (ignore-errors (jsonrpc-request conn 'bridge.shutdown (make-hash-table) :timeout 10))
      (ignore-errors (jsonrpc-shutdown conn t))))
  (setq rabbitmq-rpc--connection nil))

(defun rabbitmq-rpc--normalize-params (params)
  "Return PARAMS ready for `json-serialize'.
An empty plist becomes an empty JSON object."
  (if (null params) (make-hash-table) params))

(defun rabbitmq-rpc--error-plist (err)
  "Flatten jsonrpc error ERR into a plist with the bridge's structured data."
  (let* ((data (plist-get err :data)))
    (list :rpc-code (plist-get err :code)
          :code (or (plist-get data :code) "rpc_error")
          :message (or (plist-get data :message) (plist-get err :message) "unknown error")
          :hint (plist-get data :hint)
          :detail (plist-get data :detail)
          :ambiguous (eq (plist-get data :ambiguous) t)
          :retryable (eq (plist-get data :retryable) t))))

(defun rabbitmq-rpc-format-error (e)
  "Human-readable message for error plist E."
  (concat (plist-get e :message)
          (when (plist-get e :ambiguous)
            " [UNCERTAIN: the broker may have applied this operation; do not assume it failed]")
          (when-let* ((h (plist-get e :hint))) (concat "\n  hint: " h))))

(defun rabbitmq-rpc-default-error (e)
  "Default error handler: report E in the echo area and the events buffer."
  (message "rabbitmq: %s" (rabbitmq-rpc-format-error e))
  (run-hook-with-args 'rabbitmq-rpc-error-functions e))

(defvar rabbitmq-rpc-error-functions nil
  "Abnormal hook run with the error plist for every failed request.")

(cl-defun rabbitmq-rpc-call (method params &key then else timeout)
  "Call bridge METHOD with PARAMS asynchronously.
THEN is called with the result; ELSE with an error plist (see
`rabbitmq-rpc--error-plist').  Returns the request id, usable with
`rabbitmq-rpc-cancel'."
  (let* ((conn (rabbitmq-rpc-start))
         (else (or else #'rabbitmq-rpc-default-error))
         (m (if (symbolp method) method (intern method))))
    (car (jsonrpc-async-request
          conn m (rabbitmq-rpc--normalize-params params)
          :success-fn (lambda (result) (when then (funcall then result)))
          :error-fn (lambda (err) (funcall else (rabbitmq-rpc--error-plist err)))
          :timeout-fn (lambda ()
                        (funcall else (list :code "timeout" :ambiguous t
                                            :message (format "%s timed out locally after %ss" m (or timeout rabbitmq-rpc-timeout)))))
          :timeout (or timeout rabbitmq-rpc-timeout)))))

(defun rabbitmq-rpc-call-sync (method params &optional timeout)
  "Call METHOD with PARAMS and wait for the result.
Only for short observations (completion candidates, tests).  Signals
`user-error' on failure."
  (condition-case err
      (jsonrpc-request (rabbitmq-rpc-start)
                       (if (symbolp method) method (intern method))
                       (rabbitmq-rpc--normalize-params params)
                       :timeout (or timeout rabbitmq-rpc-timeout)
                       :cancel-on-quit #'rabbitmq-rpc-cancel)
    (jsonrpc-error
     ;; ERR is (jsonrpc-error MSG (jsonrpc-error-code . C) (jsonrpc-error-data . D) ...)
     (let* ((alist (cddr err))
            (data (alist-get 'jsonrpc-error-data alist))
            (code (and (listp data) (plist-get data :code))))
       (signal 'rabbitmq-error
               (list (or (and (listp data) (plist-get data :message))
                         (alist-get 'jsonrpc-error-message alist))
                     code data))))))

(define-error 'rabbitmq-error "RabbitMQ bridge error")

(defun rabbitmq-rpc-cancel (id)
  "Ask the bridge to cancel in-flight request ID."
  (when (rabbitmq-rpc-live-p)
    (jsonrpc-notify rabbitmq-rpc--connection '$/cancelRequest (list :id id))))

(defun rabbitmq-rpc-events-buffer ()
  "Show the raw JSON-RPC traffic log."
  (interactive)
  (if (rabbitmq-rpc-live-p)
      (display-buffer (jsonrpc-events-buffer rabbitmq-rpc--connection))
    (user-error "Bridge is not running")))

(provide 'rabbitmq-rpc)
;;; rabbitmq-rpc.el ends here
