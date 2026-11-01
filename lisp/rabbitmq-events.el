;;; rabbitmq-events.el --- Notifications, errors and the audit log  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `rabbitmq-events-mode' shows a bounded, session-tagged timeline of
;; bridge events: connection lifecycle, consumer cancellations, returned
;; messages, warnings, request errors, and audit records.

;;; Code:

(require 'rabbitmq-core)

(defcustom rabbitmq-events-max 2000
  "Maximum number of entries kept in the *rabbitmq-events* buffer."
  :type 'integer
  :group 'rabbitmq)

(defvar rabbitmq-events--count 0)

(defun rabbitmq-events--buffer ()
  (let ((b (get-buffer-create "*rabbitmq-events*")))
    (with-current-buffer b
      (unless (derived-mode-p 'rabbitmq-events-mode) (rabbitmq-events-mode)))
    b))

(defun rabbitmq-events--face (level)
  (pcase level
    ((or "error" 'error) 'rabbitmq-destructive)
    ((or "warning" 'warning) 'rabbitmq-consume)
    ("audit" 'font-lock-keyword-face)
    (_ 'default)))

(defun rabbitmq-events-log (level source text &optional session)
  "Append an event with LEVEL, SOURCE and TEXT for SESSION."
  (with-current-buffer (rabbitmq-events--buffer)
    (let ((inhibit-read-only t)
          (at-end (= (point) (point-max))))
      (save-excursion
        (goto-char (point-max))
        (insert (propertize (format-time-string "%H:%M:%S ") 'face 'shadow)
                (propertize (format "%-7s " level) 'face (rabbitmq-events--face level))
                (propertize (format "%-8s " (or source "")) 'face 'font-lock-type-face)
                (if session (propertize (format "[%s] " session) 'face 'font-lock-constant-face) "")
                (replace-regexp-in-string "\n" " " (format "%s" text))
                "\n")
        (cl-incf rabbitmq-events--count)
        (when (> rabbitmq-events--count rabbitmq-events-max)
          (goto-char (point-min))
          (forward-line (- rabbitmq-events--count rabbitmq-events-max))
          (delete-region (point-min) (point))
          (setq rabbitmq-events--count rabbitmq-events-max)))
      (when at-end (goto-char (point-max))))))

(defun rabbitmq-events--on-event (p)
  (rabbitmq-events-log (plist-get p :level) (plist-get p :source) (plist-get p :message) (plist-get p :session)))

(defun rabbitmq-events--on-session (p)
  (let* ((state (plist-get p :state))
         (detail (plist-get p :detail))
         (err (plist-get detail :error)))
    (rabbitmq-events-log (if (member state '("lost" "failed" "blocked")) "warning" "info")
                         (or (plist-get p :protocol) "session")
                         (concat state
                                 (when-let* ((n (plist-get detail :invalidated_deliveries)))
                                   (format "; %s unsettled deliveries invalidated (the broker will redeliver them)" n))
                                 (when-let* ((r (plist-get detail :reason))) (format ": %s" r))
                                 (when err (format ": %s" (if (stringp err) err (plist-get err :message)))))
                         (plist-get p :session))
    (when (member state '("lost" "failed"))
      (message "rabbitmq: session %s %s" (plist-get p :session) state))))

(defun rabbitmq-events--on-consumer (p)
  (rabbitmq-events-log (if (equal (plist-get p :state) "active") "info" "warning") "consumer"
                       (format "%s %s%s" (plist-get p :consumer) (plist-get p :state)
                               (if-let* ((r (plist-get p :reason))) (format ": %s" r) ""))
                       (plist-get p :session)))

(defun rabbitmq-events--on-returned (p)
  (rabbitmq-events-log "warning" "publish"
                       (format "returned by broker (%s %s): exchange=%S routing_key=%S"
                               (plist-get p :reply_code) (plist-get p :reply_text)
                               (plist-get p :exchange) (plist-get p :routing_key))
                       (plist-get p :session)))

(defun rabbitmq-events--on-audit (p)
  (let ((res (plist-get p :resource)))
    (rabbitmq-events-log "audit" (plist-get p :effect)
                         (format "%s %s%s%s" (plist-get p :method) (plist-get p :outcome)
                                 (if res (format " %s %s" (plist-get res :kind) (plist-get res :name)) "")
                                 (if-let* ((e (plist-get p :error))) (format ": %s" e) ""))
                         (plist-get p :profile))))

(defun rabbitmq-events--on-error (e)
  (rabbitmq-events-log (if (plist-get e :ambiguous) "warning" "error") "request" (rabbitmq-rpc-format-error e)))

(rabbitmq-rpc-on "$/event" #'rabbitmq-events--on-event)
(rabbitmq-rpc-on "$/session" #'rabbitmq-events--on-session)
(rabbitmq-rpc-on "$/consumer" #'rabbitmq-events--on-consumer)
(rabbitmq-rpc-on "$/returned" #'rabbitmq-events--on-returned)
(rabbitmq-rpc-on "$/audit" #'rabbitmq-events--on-audit)
(add-hook 'rabbitmq-rpc-error-functions #'rabbitmq-events--on-error)

(defvar-keymap rabbitmq-events-mode-map
  "c" #'rabbitmq-events-clear
  "a" #'rabbitmq-audit
  "j" #'rabbitmq-rpc-events-buffer)

(define-derived-mode rabbitmq-events-mode special-mode "RMQ-Events"
  "Timeline of RabbitMQ bridge notifications and errors.
\\{rabbitmq-events-mode-map}"
  (setq-local truncate-lines t)
  (setq header-line-format " RabbitMQ events   c clear   a audit log   j raw JSON-RPC"))

;;;###autoload
(defun rabbitmq-events ()
  "Show the RabbitMQ event timeline."
  (interactive)
  (pop-to-buffer (rabbitmq-events--buffer)))

(defun rabbitmq-events-clear ()
  "Clear the event timeline."
  (interactive)
  (with-current-buffer (rabbitmq-events--buffer)
    (let ((inhibit-read-only t)) (erase-buffer))
    (setq rabbitmq-events--count 0)))

;;;; Audit log

(define-derived-mode rabbitmq-audit-mode tabulated-list-mode "RMQ-Audit"
  "Interventions initiated through this client."
  (setq tabulated-list-format [("#" 5 t) ("Time" 9 t) ("Profile" 10 t) ("Effect" 12 t)
                               ("Method" 26 t) ("Outcome" 10 t) ("Target" 30 t) ("Error" 0 nil)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'rabbitmq-audit--refresh nil t)
  (tabulated-list-init-header))

(defun rabbitmq-audit--refresh ()
  (let ((buf (current-buffer)))
    (rabbitmq-rpc-call 'bridge.audit (list :limit 1000)
                       :then (lambda (records)
                               (when (buffer-live-p buf)
                                 (with-current-buffer buf
                                   (setq tabulated-list-entries
                                         (mapcar (lambda (r)
                                                   (let ((res (plist-get r :resource))
                                                         (out (plist-get r :outcome)))
                                                     (list (plist-get r :seq)
                                                           (vector (number-to-string (plist-get r :seq))
                                                                   (format-time-string "%H:%M:%S" (/ (plist-get r :at) 1000.0))
                                                                   (or (plist-get r :profile) "")
                                                                   (propertize (or (plist-get r :effect) "")
                                                                               'face (if (equal (plist-get r :effect) "destructive") 'rabbitmq-destructive 'default))
                                                                   (plist-get r :method)
                                                                   (propertize out 'face (pcase out ("denied" 'rabbitmq-consume) ("failed" 'rabbitmq-destructive) ("uncertain" 'rabbitmq-uncertain) (_ 'rabbitmq-observe)))
                                                                   (if res (format "%s %s" (plist-get res :kind) (plist-get res :name)) "")
                                                                   (or (plist-get r :error) "")))))
                                                 (reverse (rabbitmq--vec records))))
                                   (tabulated-list-print t)))))))

;;;###autoload
(defun rabbitmq-audit ()
  "Show the audit log of interventions made through the bridge."
  (interactive)
  (with-current-buffer (get-buffer-create "*rabbitmq-audit*")
    (rabbitmq-audit-mode)
    (rabbitmq-audit--refresh)
    (pop-to-buffer (current-buffer))))

(provide 'rabbitmq-events)
;;; rabbitmq-events.el ends here
