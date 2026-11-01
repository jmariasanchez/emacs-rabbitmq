;;; rabbitmq-clusters.el --- Session and connection management  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `rabbitmq-clusters-mode' lists configured profiles and open sessions,
;; with their connection state, broker version, epoch (reconnect count),
;; and unsettled deliveries.

;;; Code:

(require 'rabbitmq-core)

(declare-function rabbitmq-queues "rabbitmq-queues")

(defvar-keymap rabbitmq-clusters-mode-map
  "RET" #'rabbitmq-clusters-select
  "c" #'rabbitmq-clusters-connect
  "d" #'rabbitmq-clusters-disconnect
  "i" #'rabbitmq-clusters-info
  "D" #'rabbitmq-diagnostics)

(define-derived-mode rabbitmq-clusters-mode tabulated-list-mode "RMQ-Clusters"
  "Configured profiles and open sessions.
\\{rabbitmq-clusters-mode-map}"
  (setq tabulated-list-format [("Profile/Session" 22 t) ("State" 13 t) ("URI" 36 t) ("Version" 8 t)
                               ("Mode" 10 t) ("Epoch" 6 t) ("Unsettled" 9 t) ("Consumers" 9 t)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'rabbitmq-clusters--refresh nil t)
  (tabulated-list-init-header))

(defun rabbitmq-clusters--row (id info)
  (let* ((a (plist-get info :amqp091))
         (prof (plist-get info :profile))
         (state (if a (plist-get a :state) "mgmt-only"))
         (v (plist-get (plist-get info :features) :version)))
    (list id
          (vector (propertize id 'face 'bold)
                  (propertize (or state "?") 'face (pcase state ("connected" 'rabbitmq-observe) ((or "reconnecting" "blocked") 'rabbitmq-consume) ((or "failed" "closed") 'rabbitmq-destructive) (_ 'default)))
                  (format "%s@%s:%s/%s" (plist-get prof :username) (plist-get prof :host) (plist-get prof :amqp_port) (plist-get prof :vhost))
                  (if v (mapconcat #'number-to-string v ".") "?")
                  (if (eq t (plist-get info :read_only)) (propertize "read-only" 'face 'rabbitmq-read-only) "read-write")
                  (rabbitmq--str (and a (plist-get a :epoch)))
                  (rabbitmq--str (and a (plist-get a :unsettled)))
                  (rabbitmq--str (and a (length (plist-get a :consumers))))))))

(defun rabbitmq-clusters--refresh ()
  (let ((buf (current-buffer))
        (open (rabbitmq-session-names)))
    (setq tabulated-list-entries
          (append
           (mapcar (lambda (id) (rabbitmq-clusters--row id (gethash id rabbitmq--sessions))) open)
           (cl-loop for (name . pl) in rabbitmq-profiles
                    unless (member name open)
                    collect (list (concat "profile:" name)
                                  (vector name (propertize "not connected" 'face 'shadow)
                                          (format "%s@%s:%s/%s" (or (plist-get pl :username) "guest") (or (plist-get pl :host) "localhost")
                                                  (or (plist-get pl :amqp-port) 5672) (or (plist-get pl :vhost) "/"))
                                          "" (if (plist-get pl :read-only) "read-only" "read-write") "" "" "")))))
    (tabulated-list-print t)
    (dolist (id open)
      (rabbitmq-rpc-call 'session.info (list :session id)
                         :then (lambda (info)
                                 (puthash id info rabbitmq--sessions)
                                 (when (buffer-live-p buf)
                                   (with-current-buffer buf
                                     (setf (alist-get id tabulated-list-entries nil nil #'equal)
                                           (cdr (rabbitmq-clusters--row id info)))
                                     (tabulated-list-print t))))))))

;;;###autoload
(defun rabbitmq-clusters ()
  "List RabbitMQ profiles and sessions."
  (interactive)
  (with-current-buffer (get-buffer-create "*rabbitmq-clusters*")
    (rabbitmq-clusters-mode)
    (rabbitmq-clusters--refresh)
    (pop-to-buffer (current-buffer))))

(defun rabbitmq-clusters--id ()
  (or (tabulated-list-get-id) (user-error "No row at point")))

(defun rabbitmq-clusters-connect ()
  "Connect the profile at point."
  (interactive)
  (let ((id (rabbitmq-clusters--id)))
    (if (string-prefix-p "profile:" id)
        (rabbitmq-connect (substring id 8))
      (user-error "Session %s is already open" id))))

(defun rabbitmq-clusters-disconnect ()
  "Disconnect the session at point."
  (interactive)
  (let ((id (rabbitmq-clusters--id)))
    (when (string-prefix-p "profile:" id) (user-error "Not connected"))
    (rabbitmq-disconnect id)))

(defun rabbitmq-clusters-select ()
  "Make the session at point current and open the queue browser."
  (interactive)
  (let ((id (rabbitmq-clusters--id)))
    (if (string-prefix-p "profile:" id)
        (rabbitmq-connect (substring id 8))
      (setq rabbitmq--last-session id)
      (setq-default rabbitmq-current-session id)
      (let ((rabbitmq-current-session id)) (rabbitmq-queues)))))

(defun rabbitmq-clusters-info ()
  "Show full session info for the session at point."
  (interactive)
  (let ((id (rabbitmq-clusters--id)))
    (rabbitmq-rpc-call 'session.info (list :session id)
                       :then (lambda (info) (rabbitmq-show-json (format "*rabbitmq-session %s*" id) info)))))

(defun rabbitmq-show-json (name value)
  "Display VALUE as pretty-printed JSON in buffer NAME."
  (with-current-buffer (get-buffer-create name)
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert (json-serialize value :null-object nil :false-object :json-false))
      (json-pretty-print-buffer)
      (if (fboundp 'js-json-mode) (js-json-mode) (js-mode))
      (view-mode 1)
      (goto-char (point-min)))
    (pop-to-buffer (current-buffer))))

;;;###autoload
(defun rabbitmq-diagnostics ()
  "Show a redacted diagnostic report (safe to share)."
  (interactive)
  (rabbitmq-rpc-call 'bridge.diagnostics nil
                     :then (lambda (d) (rabbitmq-show-json "*rabbitmq-diagnostics*" d))))

(provide 'rabbitmq-clusters)
;;; rabbitmq-clusters.el ends here
