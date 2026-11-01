;;; rabbitmq-streams.el --- RabbitMQ Streams and super streams  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Streams are append-only logs: reading never removes or reorders
;; anything, so stream browsing needs no consent and no settlement.
;;
;; `rabbitmq-streams-browse' replays a bounded window from first, last,
;; next, an offset, or a timestamp into a messages buffer.  `n' / `p' page
;; forward and back by offset.  `rabbitmq-streams-tail' opens a live
;; subscription.  `rabbitmq-streams-mode' lists streams with retention
;; settings and leader/replica placement.

;;; Code:

(require 'rabbitmq-core)
(require 'rabbitmq-messages)

(defcustom rabbitmq-streams-page-size 100
  "Messages per stream page."
  :type 'integer
  :group 'rabbitmq)

(defvar-local rabbitmq-streams--page nil "Plist: (:stream S :first N :last N).")

(defvar-keymap rabbitmq-streams-browse-mode-map
  "n" #'rabbitmq-streams-next-page
  "p" #'rabbitmq-streams-prev-page
  "o" #'rabbitmq-streams-goto-offset
  "T" #'rabbitmq-streams-goto-time)

(define-minor-mode rabbitmq-streams-browse-mode
  "Paging keys for a stream replay buffer.
n next page, p previous page, o go to offset, T go to timestamp."
  :lighter " replay")

(defun rabbitmq-streams--read (buf stream start &optional replace)
  (rabbitmq-call 'stream.read (list :stream stream :start start :max rabbitmq-streams-page-size)
                 :then (lambda (r)
                         (when (buffer-live-p buf)
                           (with-current-buffer buf
                             (when replace
                               (clrhash rabbitmq-messages--table)
                               (setq rabbitmq-messages--order nil))
                             (setq rabbitmq-streams--page (list :stream stream :first (plist-get r :first_offset) :last (plist-get r :last_offset)))
                             (setq rabbitmq-messages--source
                                   (plist-put rabbitmq-messages--source :title
                                              (format "stream %s offsets %s..%s%s (replay, non-destructive)" stream
                                                      (or (plist-get r :first_offset) "-") (or (plist-get r :last_offset) "-")
                                                      (if (eq (plist-get r :more) t) " more" ""))))
                             (rabbitmq-messages-add buf (plist-get r :messages))
                             (when (null (rabbitmq--vec (plist-get r :messages)))
                               (message "rabbitmq: no messages in that range"))))
                         (pop-to-buffer buf))))

;;;###autoload
(defun rabbitmq-streams-browse (stream &optional start)
  "Replay STREAM from START (default: first) without consuming."
  (interactive (list (rabbitmq-read-stream) (rabbitmq-streams--read-start)))
  (let ((buf (rabbitmq-messages-buffer (format "stream %s" stream) (list :kind 'stream :stream stream))))
    (with-current-buffer buf (rabbitmq-streams-browse-mode 1))
    (rabbitmq-streams--read buf stream (or start '(:type "first")) t)))

(defun rabbitmq-streams--read-start ()
  (pcase (completing-read "Start at: " '("first" "last" "next" "offset" "timestamp") nil t nil nil "first")
    ("offset" (list :type "offset" :offset (read-number "Offset: " 0)))
    ("timestamp" (list :type "timestamp"
                       :timestamp (truncate (* 1000 (float-time (date-to-time (read-string "Time (e.g. 2026-10-31 12:00): ")))))))
    (s (list :type s))))

(defun rabbitmq-streams-next-page ()
  "Show the next page of the stream."
  (interactive)
  (let ((last (plist-get rabbitmq-streams--page :last)))
    (unless last (user-error "No current page"))
    (rabbitmq-streams--read (current-buffer) (plist-get rabbitmq-streams--page :stream) (list :type "offset" :offset (1+ last)) t)))

(defun rabbitmq-streams-prev-page ()
  "Show the previous page of the stream."
  (interactive)
  (let ((first (plist-get rabbitmq-streams--page :first)))
    (unless first (user-error "No current page"))
    (rabbitmq-streams--read (current-buffer) (plist-get rabbitmq-streams--page :stream)
                            (list :type "offset" :offset (max 0 (- first rabbitmq-streams-page-size))) t)))

(defun rabbitmq-streams-goto-offset (offset)
  "Replay from OFFSET."
  (interactive "nOffset: ")
  (rabbitmq-streams--read (current-buffer) (plist-get rabbitmq-streams--page :stream) (list :type "offset" :offset offset) t))

(defun rabbitmq-streams-goto-time (time)
  "Replay from TIME (a date string)."
  (interactive "sFrom time (e.g. 2026-10-31 12:00): ")
  (rabbitmq-streams--read (current-buffer) (plist-get rabbitmq-streams--page :stream)
                          (list :type "timestamp" :timestamp (truncate (* 1000 (float-time (date-to-time time))))) t))

;;;; Live tail

(defvar rabbitmq-streams--subs (make-hash-table :test #'equal))

(defun rabbitmq-streams--on-deliveries (p)
  (when (equal (plist-get p :protocol) "stream")
    (when-let* ((buf (gethash (plist-get p :consumer) rabbitmq-streams--subs)))
      (if (buffer-live-p buf)
          (rabbitmq-messages-add buf (plist-get p :messages) (plist-get p :dropped))
        (remhash (plist-get p :consumer) rabbitmq-streams--subs)))))
(rabbitmq-rpc-on "$/deliveries" #'rabbitmq-streams--on-deliveries)

;;;###autoload
(defun rabbitmq-streams-tail (stream &optional name)
  "Follow new messages on STREAM.  NAME enables server-side offset tracking."
  (interactive (list (rabbitmq-read-stream) (let ((n (read-string "Consumer name (empty for anonymous): "))) (unless (string-empty-p n) n))))
  (let ((buf (rabbitmq-messages-buffer (format "tail %s" stream) (list :kind 'stream :stream stream))))
    (rabbitmq-call 'stream.subscribe (append (list :stream stream :start '(:type "next")) (when name (list :name name)))
                   :then (lambda (s)
                           (puthash (plist-get s :id) buf rabbitmq-streams--subs)
                           (with-current-buffer buf
                             (setq rabbitmq-messages--source (plist-put rabbitmq-messages--source :subscription (plist-get s :id)))
                             (add-hook 'kill-buffer-hook
                                       (lambda () (ignore-errors (rabbitmq-call 'stream.unsubscribe (list :subscription (plist-get s :id)) :else #'ignore)))
                                       nil t))
                           (pop-to-buffer buf)
                           (message "rabbitmq: tailing %s (non-destructive)" stream)))))

;;;; Streams list

(defun rabbitmq-read-stream ()
  "Read a stream name with completion."
  (let* ((r (ignore-errors (rabbitmq-rpc-call-sync 'mgmt.queues (list :session (rabbitmq-session) :vhost (rabbitmq-session-vhost) :columns ["name" "type"]) 10)))
         (names (delq nil (mapcar (lambda (q) (and (equal (plist-get q :type) "stream") (plist-get q :name))) (rabbitmq--items r)))))
    (completing-read "Stream: " names nil nil)))

(defvar-keymap rabbitmq-streams-mode-map
  "RET" #'rabbitmq-streams-list-browse
  "t" #'rabbitmq-streams-list-tail
  "m" #'rabbitmq-streams-list-metadata
  "+" #'rabbitmq-stream-create
  "D" #'rabbitmq-streams-list-delete
  "o" #'rabbitmq-streams-query-offset
  "g" #'revert-buffer)

(define-derived-mode rabbitmq-streams-mode tabulated-list-mode "RMQ-Streams"
  "Streams in the session's vhost.
RET browse (replay)   t tail   m metadata   o consumer offset
+ create   D delete
\\{rabbitmq-streams-mode-map}"
  (setq tabulated-list-format [("Name" 40 t) ("Messages" 10 t) ("Max age" 8 t) ("Max bytes" 10 t)
                               ("Leader" 24 t) ("Members" 0 t)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'rabbitmq-streams--refresh nil t)
  (tabulated-list-init-header))

(defun rabbitmq-streams--refresh ()
  (let ((buf (current-buffer)))
    (rabbitmq-call 'mgmt.queues (list :vhost (rabbitmq-session-vhost))
                   :then (lambda (r)
                           (when (buffer-live-p buf)
                             (with-current-buffer buf
                               (setq header-line-format (rabbitmq--header "Streams (replay is non-destructive)" (plist-get r :fetched_at)))
                               (setq tabulated-list-entries
                                     (delq nil
                                           (mapcar (lambda (q)
                                                     (when (equal (plist-get q :type) "stream")
                                                       (let ((args (plist-get q :arguments)))
                                                         (list (plist-get q :name)
                                                               (vector (propertize (plist-get q :name) 'face 'rabbitmq-observe)
                                                                       (rabbitmq--num (plist-get q :messages))
                                                                       (rabbitmq--str (plist-get args :x-max-age))
                                                                       (rabbitmq--str (plist-get args :x-max-length-bytes))
                                                                       (or (plist-get q :leader) "")
                                                                       (rabbitmq--str (plist-get q :members)))))))
                                                   (rabbitmq--items r))))
                               (tabulated-list-print t)))))))

;;;###autoload
(defun rabbitmq-streams ()
  "List streams."
  (interactive)
  (rabbitmq-with-buffer (format "*rabbitmq-streams %s*" (rabbitmq-session)) 'rabbitmq-streams-mode
    (rabbitmq-streams--refresh)))

(defun rabbitmq-streams--at-point () (or (tabulated-list-get-id) (user-error "No stream at point")))
(defun rabbitmq-streams-list-browse () "Replay the stream at point." (interactive) (rabbitmq-streams-browse (rabbitmq-streams--at-point) (rabbitmq-streams--read-start)))
(defun rabbitmq-streams-list-tail () "Tail the stream at point." (interactive) (rabbitmq-streams-tail (rabbitmq-streams--at-point)))

(declare-function rabbitmq-show-json "rabbitmq-clusters")

(defun rabbitmq-streams-list-metadata ()
  "Show leader and replicas of the stream at point."
  (interactive)
  (let ((s (rabbitmq-streams--at-point)))
    (rabbitmq-call 'stream.metadata (list :stream s) :then (lambda (r) (rabbitmq-show-json (format "*rabbitmq-stream-meta %s*" s) r)))))

(defun rabbitmq-streams-query-offset (consumer)
  "Show the stored offset of CONSUMER on the stream at point."
  (interactive "sConsumer name: ")
  (let ((s (rabbitmq-streams--at-point)))
    (rabbitmq-call 'stream.query_offset (list :stream s :consumer consumer)
                   :then (lambda (o) (message "rabbitmq: %s on %s: %s" consumer s (if o (format "offset %s" o) "no stored offset"))))))

(defun rabbitmq-stream-create (name &optional max-age max-bytes partitions)
  "Create stream NAME with retention MAX-AGE (e.g. 7D) and MAX-BYTES.
With PARTITIONS, create a super stream."
  (interactive (list (read-string "Stream name: ")
                     (let ((a (read-string "Max age (e.g. 7D, empty for none): "))) (unless (string-empty-p a) a))
                     (let ((b (read-string "Max length bytes (empty for none): "))) (unless (string-empty-p b) (string-to-number b)))
                     (let ((p (read-string "Super stream partitions (empty for a plain stream): "))) (unless (string-empty-p p) (string-to-number p)))))
  (rabbitmq-call 'stream.create (append (list :name name)
                                        (when max-age (list :max_age max-age))
                                        (when max-bytes (list :max_length_bytes max-bytes))
                                        (when partitions (list :partitions partitions)))
                 :then (lambda (_) (message "rabbitmq: created %s %s" (if partitions "super stream" "stream") name)
                         (dolist (b (buffer-list)) (with-current-buffer b (when (derived-mode-p 'rabbitmq-streams-mode) (revert-buffer)))))))

(defun rabbitmq-streams-list-delete ()
  "Delete the stream at point (typed confirmation)."
  (interactive)
  (let ((s (rabbitmq-streams--at-point)))
    (rabbitmq-confirm-destructive "stream" s "deletes the stream and its entire log")
    (rabbitmq-call 'stream.delete (list :name s :confirm s)
                   :then (lambda (_) (message "rabbitmq: deleted stream %s" s) (revert-buffer)))))

(provide 'rabbitmq-streams)
;;; rabbitmq-streams.el ends here
