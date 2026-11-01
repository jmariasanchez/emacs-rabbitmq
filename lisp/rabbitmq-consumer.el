;;; rabbitmq-consumer.el --- Consumer lifecycle and acknowledgement  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Interactive consumers.  `rabbitmq-consumer-start' asks for consent,
;; starts a manual-ack consumer with a small prefetch, and opens a messages
;; buffer that fills from `$/deliveries' notifications.  Prefetch bounds
;; how many unacked messages the broker will push, so an idle display
;; never accumulates an unbounded backlog.  Pausing stops display updates;
;; deliveries stay buffered (bounded) in the bridge.
;;
;; `rabbitmq-consumers-mode' lists the session's consumers.

;;; Code:

(require 'rabbitmq-core)
(require 'rabbitmq-messages)

(defcustom rabbitmq-consumer-default-prefetch 10
  "Default prefetch for interactive consumers."
  :type 'integer
  :group 'rabbitmq)

(defvar rabbitmq-consumer--buffers (make-hash-table :test #'equal)
  "Map \"session|consumer-tag\" to the messages buffer receiving deliveries.")

(defun rabbitmq-consumer--key (session tag) (concat session "|" tag))

(defun rabbitmq-consumer--on-deliveries (p)
  (let* ((key (rabbitmq-consumer--key (plist-get p :session) (plist-get p :consumer)))
         (buf (gethash key rabbitmq-consumer--buffers)))
    (when buf
      (if (buffer-live-p buf)
          (rabbitmq-messages-add buf (plist-get p :messages) (plist-get p :dropped))
        (remhash key rabbitmq-consumer--buffers)))))

(defun rabbitmq-consumer--on-consumer (p)
  (let* ((key (rabbitmq-consumer--key (plist-get p :session) (plist-get p :consumer)))
         (buf (gethash key rabbitmq-consumer--buffers)))
    (when (and buf (buffer-live-p buf) (not (equal (plist-get p :state) "active")))
      (with-current-buffer buf
        (setq rabbitmq-messages--source (plist-put rabbitmq-messages--source :state (plist-get p :state)))
        (setq rabbitmq-messages--source
              (plist-put rabbitmq-messages--source :title
                         (format "%s [%s]" (plist-get rabbitmq-messages--source :queue) (plist-get p :state))))
        (rabbitmq-messages--render))
      (message "rabbitmq: consumer %s %s%s" (plist-get p :consumer) (plist-get p :state)
               (if-let* ((r (plist-get p :reason))) (format ": %s" r) "")))))

(rabbitmq-rpc-on "$/deliveries" #'rabbitmq-consumer--on-deliveries)
(rabbitmq-rpc-on "$/consumer" #'rabbitmq-consumer--on-consumer)

;;;###autoload
(defun rabbitmq-consumer-start (queue &optional prefetch auto-ack)
  "Start an interactive consumer on QUEUE after asking consent.
PREFETCH limits unacked deliveries.  AUTO-ACK (prefix argument) removes
messages immediately and is strongly discouraged for inspection."
  (interactive (list (rabbitmq-read-queue "Consume from queue: ")
                     (when current-prefix-arg (read-number "Prefetch: " rabbitmq-consumer-default-prefetch))
                     (and current-prefix-arg (y-or-n-p "Auto-ack (messages are removed as they arrive)? "))))
  (rabbitmq-consent-consume queue (if auto-ack "Consuming with auto-ack" "Consuming"))
  (let* ((session (rabbitmq-session))
         (buf (rabbitmq-messages-buffer (format "consume %s" queue) (list :kind 'consumer :queue queue))))
    (rabbitmq-call 'consumer.start (list :queue queue :consent t
                                         :prefetch (or prefetch rabbitmq-consumer-default-prefetch)
                                         :auto_ack (if auto-ack t :json-false))
                   :then (lambda (c)
                           (let ((tag (plist-get c :tag)))
                             (puthash (rabbitmq-consumer--key session tag) buf rabbitmq-consumer--buffers)
                             (with-current-buffer buf
                               (setq rabbitmq-messages--source (append rabbitmq-messages--source (list :consumer tag)))
                               (rabbitmq-consumer-live-mode 1)
                               (add-hook 'kill-buffer-hook #'rabbitmq-consumer--cancel-on-kill nil t))
                             (pop-to-buffer buf)
                             (message "rabbitmq: consuming %s as %s (prefetch %s, %s)" queue tag (plist-get c :prefetch)
                                      (if (eq (plist-get c :auto_ack) t) "AUTO-ACK" "manual ack")))))))

(defun rabbitmq-consumer--tag ()
  (or (plist-get rabbitmq-messages--source :consumer) (user-error "Not a consumer buffer")))

(defun rabbitmq-consumer-cancel ()
  "Cancel this buffer's consumer.  Unsettled deliveries stay settleable."
  (interactive)
  (let ((tag (rabbitmq-consumer--tag)))
    (rabbitmq-call 'consumer.cancel (list :consumer tag)
                   :then (lambda (_) (message "rabbitmq: consumer %s cancelled; unsettled messages can still be settled here" tag)))))

(defun rabbitmq-consumer--cancel-on-kill ()
  (when-let* ((tag (plist-get rabbitmq-messages--source :consumer)))
    (when (rabbitmq-rpc-live-p)
      (ignore-errors (rabbitmq-call 'consumer.cancel (list :consumer tag) :else #'ignore)))
    (remhash (rabbitmq-consumer--key rabbitmq-current-session tag) rabbitmq-consumer--buffers)))

(defun rabbitmq-consumer-pause ()
  "Pause display updates (deliveries are buffered in the bridge, bounded)."
  (interactive)
  (rabbitmq-call 'consumer.pause (list :consumer (rabbitmq-consumer--tag))
                 :then (lambda (_) (message "rabbitmq: paused; prefetch limits what the broker sends meanwhile"))))

(defun rabbitmq-consumer-resume ()
  "Resume display updates and flush buffered deliveries."
  (interactive)
  (rabbitmq-call 'consumer.resume (list :consumer (rabbitmq-consumer--tag))
                 :then (lambda (_) (message "rabbitmq: resumed"))))

(defvar-keymap rabbitmq-consumer-live-mode-map
  "C-c C-k" #'rabbitmq-consumer-cancel
  "C-c C-p" #'rabbitmq-consumer-pause
  "C-c C-r" #'rabbitmq-consumer-resume)

(define-minor-mode rabbitmq-consumer-live-mode
  "Keys for a live consumer buffer: C-c C-k cancel, C-c C-p pause, C-c C-r resume."
  :lighter " live")

;;;; Consumers list

(define-derived-mode rabbitmq-consumers-mode tabulated-list-mode "RMQ-Consumers"
  "Consumers known to the broker (Management API)."
  (setq tabulated-list-format [("Tag" 30 t) ("Queue" 30 t) ("Ack" 6 t) ("Prefetch" 8 t)
                               ("Active" 7 t) ("Status" 12 t) ("Exclusive" 9 t) ("Connection" 0 t)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'rabbitmq-consumers--refresh nil t)
  (tabulated-list-init-header))

(defun rabbitmq-consumers--refresh ()
  (let ((buf (current-buffer)))
    (rabbitmq-call 'mgmt.consumers (list :vhost (rabbitmq-session-vhost))
                   :then (lambda (r)
                           (when (buffer-live-p buf)
                             (with-current-buffer buf
                               (setq header-line-format (rabbitmq--header "Consumers" (plist-get r :fetched_at)))
                               (setq tabulated-list-entries
                                     (mapcar (lambda (c)
                                               (list (plist-get c :consumer_tag)
                                                     (vector (plist-get c :consumer_tag)
                                                             (or (plist-get (plist-get c :queue) :name) "")
                                                             (if (eq (plist-get c :ack_required) t) "manual" "auto")
                                                             (rabbitmq--str (plist-get c :prefetch_count))
                                                             (rabbitmq--str (plist-get c :active))
                                                             (or (plist-get c :activity_status) "")
                                                             (rabbitmq--str (plist-get c :exclusive))
                                                             (or (plist-get (plist-get c :channel_details) :connection_name) ""))))
                                             (rabbitmq--items r)))
                               (tabulated-list-print t)))))))

;;;###autoload
(defun rabbitmq-consumers ()
  "List consumers in the session's vhost."
  (interactive)
  (rabbitmq-with-buffer (format "*rabbitmq-consumers %s*" (rabbitmq-session)) 'rabbitmq-consumers-mode
    (rabbitmq-consumers--refresh)))

(declare-function rabbitmq-read-queue "rabbitmq-queues")

(provide 'rabbitmq-consumer)
;;; rabbitmq-consumer.el ends here
