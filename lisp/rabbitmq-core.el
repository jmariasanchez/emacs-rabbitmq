;;; rabbitmq-core.el --- Shared state and helpers for rabbitmq.el  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Profiles, sessions, the safety UX (consent and typed confirmation), and
;; helpers shared by every RabbitMQ mode.  Mode files require this file;
;; `rabbitmq.el' requires the mode files.

;;; Code:

(require 'cl-lib)
(require 'subr-x)
(require 'auth-source)
(require 'rabbitmq-rpc)

;;;; Customization

(defcustom rabbitmq-profiles
  '(("local" :host "localhost" :management-url "http://localhost:15672"))
  "Connection profiles: list of (NAME . PLIST).
Recognised keys:

  :host :amqp-port :amqp10-port :stream-port :vhost :username
  :password            literal (discouraged) or omit to use auth-source
  :management-url      e.g. \"http://localhost:15672\"
  :tls                 plist (:enabled t :ca-file F :client-cert-file F
                       :client-key-file F :verify t)
  :auth-mechanism      plain | amqplain | external
  :heartbeat           seconds
  :connection-name     shown in the management UI
  :read-only           non-nil rejects every intervention
  :protected           list of (:kind K :vhost V :name GLOB :protection P)
                       with P = no_destructive | read_only
  :limits              plist of bridge limits (:max-buffered-deliveries ...)
  :reconnect-attempts  integer

Passwords are looked up with `auth-source' using :host and :username
(and port \"amqp\") when :password is absent."
  :type '(alist :key-type string :value-type plist)
  :group 'rabbitmq)

(defcustom rabbitmq-default-profile nil
  "Profile name used by `rabbitmq-connect' without a prefix argument."
  :type '(choice (const nil) string)
  :group 'rabbitmq)

(defface rabbitmq-destructive '((t :inherit error :weight bold))
  "Face for destructive actions and warnings."
  :group 'rabbitmq)
(defface rabbitmq-consume '((t :inherit warning :weight bold))
  "Face marking state-altering reads (consuming from ordinary queues)."
  :group 'rabbitmq)
(defface rabbitmq-observe '((t :inherit success))
  "Face marking non-destructive observation."
  :group 'rabbitmq)
(defface rabbitmq-read-only '((t :inherit font-lock-comment-face :slant italic))
  "Face for read-only profile indicators."
  :group 'rabbitmq)
(defface rabbitmq-stale '((t :inherit shadow))
  "Face for stale or unavailable metrics."
  :group 'rabbitmq)
(defface rabbitmq-uncertain '((t :inherit warning :underline t))
  "Face for operations whose outcome is uncertain."
  :group 'rabbitmq)

;;;; Sessions

(defvar rabbitmq--sessions (make-hash-table :test #'equal)
  "Open sessions: id -> info plist from the bridge.")

(defvar rabbitmq-current-session nil
  "Session id used by commands in the current buffer.
Buffer-local in RabbitMQ buffers; the global value is the last
connected session.")
(make-variable-buffer-local 'rabbitmq-current-session)

(defvar rabbitmq--last-session nil "Most recently used session id.")

(defvar rabbitmq-connect-hook nil
  "Hook run with the session id after a successful connect.")

(defun rabbitmq--snake (kw)
  "Convert keyword KW like :management-url to the JSON key \"management_url\"."
  (intern (concat ":" (replace-regexp-in-string "-" "_" (substring (symbol-name kw) 1)))))

(defun rabbitmq--plist-to-json (plist)
  "Convert a kebab-case PLIST to a snake_case plist for the bridge."
  (cl-loop for (k v) on plist by #'cddr
           append (list (rabbitmq--snake k)
                        (cond ((eq v t) t)
                              ((and (listp v) (keywordp (car-safe v))) (rabbitmq--plist-to-json v))
                              ((and (listp v) (listp (car-safe v)) (keywordp (car-safe (car v))))
                               (vconcat (mapcar #'rabbitmq--plist-to-json v)))
                              ((and (symbolp v) v (not (keywordp v))) (symbol-name v))
                              (t v)))))

(defun rabbitmq--auth-password (host user)
  "Look up the password for USER at HOST with auth-source."
  (when-let* ((found (car (auth-source-search :host host :user user :port '("amqp" "5672" "rabbitmq") :max 1)))
              (secret (plist-get found :secret)))
    (if (functionp secret) (funcall secret) secret)))

(defun rabbitmq-profile-json (name)
  "Build the bridge profile object for profile NAME, resolving secrets."
  (let* ((entry (or (assoc name rabbitmq-profiles) (user-error "No profile %s in `rabbitmq-profiles'" name)))
         (plist (copy-sequence (cdr entry)))
         (host (or (plist-get plist :host) "localhost"))
         (user (or (plist-get plist :username) "guest")))
    (unless (plist-member plist :password)
      (setq plist (plist-put plist :password
                             (or (rabbitmq--auth-password host user)
                                 (and (equal user "guest") "guest")
                                 (read-passwd (format "RabbitMQ password for %s@%s: " user host))))))
    (unless (plist-member plist :read-only)
      (setq plist (plist-put plist :read-only :json-false)))
    (append (list :name name) (rabbitmq--plist-to-json plist))))

(defun rabbitmq-session-names ()
  "Names of open sessions."
  (hash-table-keys rabbitmq--sessions))

(defun rabbitmq-session (&optional session)
  "Return SESSION, the buffer's session, or prompt for one."
  (or session
      (and rabbitmq-current-session (gethash rabbitmq-current-session rabbitmq--sessions) rabbitmq-current-session)
      (and rabbitmq--last-session (gethash rabbitmq--last-session rabbitmq--sessions) rabbitmq--last-session)
      (let ((names (rabbitmq-session-names)))
        (cond ((null names) (call-interactively #'rabbitmq-connect) rabbitmq--last-session)
              ((null (cdr names)) (car names))
              (t (completing-read "Session: " names nil t))))))

(defun rabbitmq-session-info (&optional session)
  "Cached bridge info plist for SESSION."
  (gethash (rabbitmq-session session) rabbitmq--sessions))

(defun rabbitmq-session-read-only-p (&optional session)
  "Non-nil when SESSION's profile is read-only."
  (eq t (plist-get (rabbitmq-session-info session) :read_only)))

(defun rabbitmq-session-vhost (&optional session)
  "Default vhost of SESSION."
  (or (plist-get (plist-get (rabbitmq-session-info session) :profile) :vhost) "/"))

;;;###autoload
(defun rabbitmq-connect (profile &optional session-name)
  "Open a session for PROFILE (a name in `rabbitmq-profiles').
With prefix argument, also prompt for SESSION-NAME."
  (interactive
   (list (if (and rabbitmq-default-profile (not current-prefix-arg))
             rabbitmq-default-profile
           (completing-read "Profile: " (mapcar #'car rabbitmq-profiles) nil t nil nil
                            (or rabbitmq-default-profile (caar rabbitmq-profiles))))
         (when current-prefix-arg (read-string "Session name: "))))
  (let* ((id (if (and session-name (not (string-empty-p session-name))) session-name profile))
         (json (rabbitmq-profile-json profile)))
    (when (gethash id rabbitmq--sessions)
      (user-error "Session %s is already open" id))
    (message "rabbitmq: connecting %s..." id)
    (rabbitmq-rpc-call
     'session.open (list :session id :profile json)
     :timeout 60
     :then (lambda (info)
             (puthash id info rabbitmq--sessions)
             (setq rabbitmq--last-session id)
             (setq-default rabbitmq-current-session id)
             (let ((warnings (append (plist-get info :warnings) nil))
                   (version (plist-get (plist-get info :features) :version)))
               (message "rabbitmq: connected %s%s%s%s" id
                        (if version (format " (RabbitMQ %s)" (mapconcat #'number-to-string version ".")) "")
                        (if (eq t (plist-get info :read_only)) (propertize " [read-only]" 'face 'rabbitmq-read-only) "")
                        (if warnings (concat "; " (string-join warnings "; ")) "")))
             (run-hook-with-args 'rabbitmq-connect-hook id)))))

(defun rabbitmq-disconnect (session)
  "Close SESSION and release its broker resources."
  (interactive (list (completing-read "Disconnect session: " (rabbitmq-session-names) nil t)))
  (rabbitmq-rpc-call 'session.close (list :session session)
                     :then (lambda (_)
                             (remhash session rabbitmq--sessions)
                             (when (equal rabbitmq--last-session session) (setq rabbitmq--last-session nil))
                             (message "rabbitmq: closed %s" session))))

(defun rabbitmq--forget-sessions ()
  "Clear session state when the bridge exits."
  (when (> (hash-table-count rabbitmq--sessions) 0)
    (message "rabbitmq: bridge exited; %d session(s) closed" (hash-table-count rabbitmq--sessions)))
  (clrhash rabbitmq--sessions))
(add-hook 'rabbitmq-rpc-shutdown-hook #'rabbitmq--forget-sessions)

(defun rabbitmq-refresh-session-info (&optional session)
  "Refresh cached info for SESSION."
  (let ((s (rabbitmq-session session)))
    (rabbitmq-rpc-call 'session.info (list :session s)
                       :then (lambda (info) (puthash s info rabbitmq--sessions)))))

;;;; Safety UX

(defun rabbitmq-confirm-destructive (kind name &optional detail)
  "Ask the user to type NAME to confirm a destructive operation on KIND.
Return NAME on success, signal `user-error' otherwise."
  (when (rabbitmq-session-read-only-p)
    (user-error "Session %s is read-only" (rabbitmq-session)))
  (let ((typed (read-string
                (concat (propertize (format "DESTRUCTIVE: %s %s" kind name) 'face 'rabbitmq-destructive)
                        (if detail (format " (%s)" detail) "")
                        (format "\nType the %s name to confirm: " kind)))))
    (unless (equal typed name)
      (user-error "Confirmation did not match; nothing was done"))
    name))

(defun rabbitmq-consent-consume (queue what)
  "Ask for consent to consume from ordinary QUEUE.  WHAT describes the action."
  (when (rabbitmq-session-read-only-p)
    (user-error "Session %s is read-only" (rabbitmq-session)))
  (unless (yes-or-no-p
           (concat (propertize (format "%s from queue %s " what queue) 'face 'rabbitmq-consume)
                   "removes messages or makes them unacknowledged, and may change ordering and redelivery state.  Continue? "))
    (user-error "Cancelled; queue %s was not touched" queue))
  t)

(defun rabbitmq-call (method params &rest args)
  "Call METHOD with PARAMS in the current session.
ARGS are passed to `rabbitmq-rpc-call' (:then :else :timeout)."
  (apply #'rabbitmq-rpc-call method (append (list :session (rabbitmq-session)) params) args))

;;;; Helpers shared by modes

(defun rabbitmq--str (v)
  "Render V for a table cell."
  (cond ((null v) "")
        ((eq v :json-false) "no")
        ((eq v t) "yes")
        ((stringp v) v)
        ((numberp v) (number-to-string v))
        ((vectorp v) (mapconcat #'rabbitmq--str v ","))
        (t (format "%S" v))))

(defun rabbitmq--num (v)
  "Render number V compactly, or \"?\" when unavailable."
  (cond ((null v) (propertize "?" 'face 'rabbitmq-stale))
        ((not (numberp v)) (rabbitmq--str v))
        ((>= v 1000000000) (format "%.1fG" (/ v 1e9)))
        ((>= v 1000000) (format "%.1fM" (/ v 1e6)))
        ((>= v 10000) (format "%.1fk" (/ v 1e3)))
        ((floatp v) (format "%.1f" v))
        (t (number-to-string v))))

(defun rabbitmq--bytes (v)
  "Render byte count V."
  (if (numberp v) (file-size-human-readable v) (propertize "?" 'face 'rabbitmq-stale)))

(defun rabbitmq--age (ms)
  "Human age string for epoch milliseconds MS."
  (if (numberp ms)
      (let ((s (/ (- (* 1000 (float-time)) ms) 1000.0)))
        (cond ((< s 60) (format "%ds ago" s))
              ((< s 3600) (format "%dm ago" (/ s 60)))
              (t (format "%dh ago" (/ s 3600)))))
    "never"))

(defun rabbitmq--vec (v)
  "Coerce V (vector, list, or nil) to a list."
  (cond ((vectorp v) (append v nil)) ((listp v) v) (t nil)))

(defun rabbitmq--items (result)
  "Return the list of items from a sampled/paged RESULT."
  (let ((data (if (plist-member result :data) (plist-get result :data) result)))
    (rabbitmq--vec (if (and (listp data) (plist-member data :items)) (plist-get data :items) data))))

(defun rabbitmq-read-json-object (prompt &optional initial)
  "Read a JSON object with PROMPT and return a plist (or nil for empty)."
  (let ((s (string-trim (read-string prompt initial))))
    (if (string-empty-p s) nil
      (json-parse-string s :object-type 'plist :null-object nil :false-object :json-false))))

(defun rabbitmq--empty-obj () (make-hash-table))

(defmacro rabbitmq-with-buffer (name mode &rest body)
  "Pop to buffer NAME in MODE, bound to the current session, and run BODY."
  (declare (indent 2))
  `(let ((session (rabbitmq-session)))
     (with-current-buffer (get-buffer-create ,name)
       (unless (derived-mode-p ,mode) (funcall ,mode))
       (setq rabbitmq-current-session session)
       ,@body
       (pop-to-buffer (current-buffer)))))

(defcustom rabbitmq-auto-refresh-interval nil
  "Seconds between automatic refreshes of list buffers, or nil to disable."
  :type '(choice (const nil) number)
  :group 'rabbitmq)

(defvar-local rabbitmq--refresh-timer nil)
(defvar-local rabbitmq--last-refresh nil "Epoch ms of the last successful refresh.")

(defun rabbitmq-auto-refresh-mode-setup ()
  "Start the auto-refresh timer in this buffer when configured."
  (when (and rabbitmq-auto-refresh-interval (not rabbitmq--refresh-timer))
    (let ((buf (current-buffer)))
      (setq rabbitmq--refresh-timer
            (run-with-timer rabbitmq-auto-refresh-interval rabbitmq-auto-refresh-interval
                            (lambda ()
                              (if (buffer-live-p buf)
                                  (when (get-buffer-window buf 'visible)
                                    (with-current-buffer buf (revert-buffer)))
                                (cancel-function-timers 'ignore)))))
      (add-hook 'kill-buffer-hook (lambda () (when rabbitmq--refresh-timer (cancel-timer rabbitmq--refresh-timer))) nil t))))

(defun rabbitmq--header (title &optional sampled)
  "Header line for TITLE, showing the session and SAMPLED age."
  (concat " " (propertize title 'face 'bold)
          "  session: " (or rabbitmq-current-session "-")
          (if (rabbitmq-session-read-only-p rabbitmq-current-session)
              (propertize "  [read-only]" 'face 'rabbitmq-read-only) "")
          (if sampled (propertize (format "  sampled %s" (rabbitmq--age sampled)) 'face 'rabbitmq-stale) "")))

(provide 'rabbitmq-core)
;;; rabbitmq-core.el ends here
