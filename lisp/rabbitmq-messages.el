;;; rabbitmq-messages.el --- Message and delivery inspection  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `rabbitmq-messages-mode' is a bounded list of inbound messages from any
;; source (basic.get, consumers, stream reads, AMQP 1.0 receivers).  Each
;; row shows whether the message is still unsettled and which channel or
;; link owns it.  Settlement keys (a ack, n nack, r reject, R requeue) act on
;; the marked messages or the message at point and always go through the
;; bridge, which settles on the original channel and refuses stale tags.
;;
;; RET shows the full message with properties, headers, and the payload as
;; text, pretty JSON, hex, or base64.

;;; Code:

(require 'rabbitmq-core)

(declare-function rabbitmq-read-queue "rabbitmq-queues")

(defcustom rabbitmq-messages-max 1000
  "Maximum messages kept in a messages buffer; older ones scroll out."
  :type 'integer
  :group 'rabbitmq)

(defvar-local rabbitmq-messages--source nil
  "Plist describing where messages come from.
Keys include :kind (get, consumer, stream, amqp10) and :title.")
(defvar-local rabbitmq-messages--table nil "Hash: id -> message plist.")
(defvar-local rabbitmq-messages--order nil "Ids in arrival order (newest last).")
(defvar-local rabbitmq-messages--settled nil "Hash: id -> settlement outcome string.")
(defvar-local rabbitmq-messages--dropped 0 "Messages dropped from view because of limits.")

(defvar-keymap rabbitmq-messages-mode-map
  "RET" #'rabbitmq-messages-show
  "a" #'rabbitmq-messages-ack
  "n" #'rabbitmq-messages-nack
  "r" #'rabbitmq-messages-reject
  "R" #'rabbitmq-messages-requeue
  "m" #'rabbitmq-messages-mark
  "u" #'rabbitmq-messages-unmark
  "U" #'rabbitmq-messages-unmark-all
  "M" #'rabbitmq-messages-mark-all-unsettled
  "w" #'rabbitmq-messages-copy-payload
  "e" #'rabbitmq-messages-export
  "=" #'rabbitmq-messages-diff
  "x" #'rabbitmq-messages-republish
  "c" #'rabbitmq-messages-clear-settled)

(define-derived-mode rabbitmq-messages-mode tabulated-list-mode "RMQ-Messages"
  "Inspect messages and settle deliveries.

Settlement acts on marked messages, or the message at point:
  a ack   n nack (dead-letter or drop)   R nack with requeue   r reject
Requeued messages return to the queue marked redelivered and may change
position.  Stream messages need no settlement.

\\{rabbitmq-messages-mode-map}"
  (setq tabulated-list-format [("M" 1 nil) ("#" 5 t) ("State" 10 t) ("Redeliv" 7 t) ("Routing key" 24 t)
                               ("Type" 10 t) ("Size" 7 t) ("Owner" 18 t) ("Payload" 0 nil)])
  (setq tabulated-list-padding 1)
  (setq rabbitmq-messages--table (make-hash-table :test #'equal))
  (setq rabbitmq-messages--settled (make-hash-table :test #'equal))
  (add-hook 'tabulated-list-revert-hook #'rabbitmq-messages--render nil t)
  (tabulated-list-init-header))

(defun rabbitmq-messages--state (m)
  (let* ((id (plist-get m :id))
         (src (plist-get m :source))
         (kind (plist-get src :kind))
         (settled (gethash id rabbitmq-messages--settled)))
    (cond (settled settled)
          ((equal kind "stream") "replayed")
          ((eq (plist-get src :auto_ack) t) "auto-acked")
          ((eq (plist-get src :settled) t) "settled")
          (t "unsettled"))))

(defun rabbitmq-messages--owner (src)
  (pcase (plist-get src :kind)
    ("amqp091" (format "ch%s/tag%s/e%s" (plist-get src :channel) (plist-get src :delivery_tag) (plist-get src :connection_epoch)))
    ("stream" (format "offset %s" (plist-get src :offset)))
    ("amqp10" (format "link#%s" (plist-get src :delivery_id)))
    (_ "")))

(defun rabbitmq-messages--preview (payload)
  (let* ((enc (plist-get payload :encoding))
         (data (or (plist-get payload :data) "")))
    (if (equal enc "utf8")
        (truncate-string-to-width (replace-regexp-in-string "[\n\t ]+" " " data) 200)
      (propertize (format "<%s %s bytes>" enc (plist-get payload :size)) 'face 'shadow))))

(defun rabbitmq-messages--render ()
  (setq tabulated-list-entries
        (cl-loop for id in rabbitmq-messages--order
                 for i from 1
                 for m = (gethash id rabbitmq-messages--table)
                 when m
                 collect (let* ((src (plist-get m :source))
                                (state (rabbitmq-messages--state m))
                                (p (plist-get m :payload))
                                (props (plist-get m :properties)))
                           (list id
                                 (vector (if (plist-get m :marked) "*" " ")
                                         (number-to-string i)
                                         (propertize state 'face (pcase state
                                                                   ("unsettled" 'rabbitmq-consume)
                                                                   ("stale" 'rabbitmq-destructive)
                                                                   ("replayed" 'rabbitmq-observe)
                                                                   (_ 'shadow)))
                                         (if (eq (plist-get m :redelivered) t) (propertize "yes" 'face 'warning) "")
                                         (or (plist-get m :routing_key) (plist-get src :stream) "")
                                         (or (plist-get props :content_type) "")
                                         (rabbitmq--bytes (plist-get p :size))
                                         (rabbitmq-messages--owner src)
                                         (concat (if (eq (plist-get p :truncated) t) (propertize "[truncated] " 'face 'warning) "")
                                                 (rabbitmq-messages--preview p)))))))
  (setq header-line-format
        (concat (rabbitmq--header (plist-get rabbitmq-messages--source :title))
                (format "  %d message(s)" (length rabbitmq-messages--order))
                (let ((u (cl-count-if (lambda (id) (equal "unsettled" (rabbitmq-messages--state (gethash id rabbitmq-messages--table)))) rabbitmq-messages--order)))
                  (if (> u 0) (propertize (format "  %d unsettled" u) 'face 'rabbitmq-consume) ""))
                (if (> rabbitmq-messages--dropped 0)
                    (propertize (format "  %d dropped from view" rabbitmq-messages--dropped) 'face 'warning) "")))
  (tabulated-list-print t))

(defun rabbitmq-messages-add (buffer messages &optional dropped)
  "Append MESSAGES (vector or list of plists) to BUFFER and re-render.
DROPPED is the bridge-reported drop count."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (dolist (m (rabbitmq--vec messages))
        (let ((id (plist-get m :id)))
          (unless (gethash id rabbitmq-messages--table)
            (setq rabbitmq-messages--order (nconc rabbitmq-messages--order (list id))))
          (puthash id m rabbitmq-messages--table)))
      (while (> (length rabbitmq-messages--order) rabbitmq-messages-max)
        (let ((old (pop rabbitmq-messages--order)))
          (remhash old rabbitmq-messages--table)
          (remhash old rabbitmq-messages--settled)
          (cl-incf rabbitmq-messages--dropped)))
      (when (and dropped (numberp dropped))
        (setq rabbitmq-messages--dropped (max rabbitmq-messages--dropped dropped)))
      (rabbitmq-messages--render))))

(defun rabbitmq-messages-buffer (title source)
  "Create a messages buffer named after TITLE with SOURCE plist."
  (let ((session (rabbitmq-session)))
    (with-current-buffer (generate-new-buffer (format "*rabbitmq-messages %s*" title))
      (rabbitmq-messages-mode)
      (setq rabbitmq-current-session session)
      (setq rabbitmq-messages--source (append (list :title title) source))
      (rabbitmq-messages--render)
      (current-buffer))))

;;;###autoload
(defun rabbitmq-messages-get (queue &optional count auto-ack)
  "Fetch COUNT messages from QUEUE with basic.get after asking consent.
Messages stay unacknowledged until settled unless AUTO-ACK."
  (interactive (list (rabbitmq-read-queue "Fetch from queue: ")
                     (read-number "How many: " 1)
                     (and current-prefix-arg (y-or-n-p "Auto-ack (remove immediately, cannot be settled)? "))))
  (rabbitmq-consent-consume queue (if auto-ack "Fetching (auto-ack)" "Fetching"))
  (let ((buf (rabbitmq-messages-buffer (format "get %s" queue) (list :kind 'get :queue queue))))
    (rabbitmq-call 'message.get (list :queue queue :count (or count 1) :consent t :auto_ack (if auto-ack t :json-false))
                   :then (lambda (r)
                           (rabbitmq-messages-add buf (plist-get r :messages))
                           (pop-to-buffer buf)
                           (message "rabbitmq: %s%s" (if (eq (plist-get r :empty) t) "queue is empty. " "")
                                    (plist-get r :note))))))

;;;; Selection and settlement

(defun rabbitmq-messages--marked ()
  (or (cl-remove-if-not (lambda (id) (plist-get (gethash id rabbitmq-messages--table) :marked)) rabbitmq-messages--order)
      (when-let* ((id (tabulated-list-get-id))) (list id))
      (user-error "No message at point")))

(defun rabbitmq-messages--set-mark (id on)
  (let ((m (gethash id rabbitmq-messages--table)))
    (puthash id (plist-put m :marked on) rabbitmq-messages--table)
    (tabulated-list-set-col 0 (if on "*" " ") t)))

(defun rabbitmq-messages-mark ()
  "Mark the message at point."
  (interactive)
  (when-let* ((id (tabulated-list-get-id))) (rabbitmq-messages--set-mark id t))
  (forward-line 1))

(defun rabbitmq-messages-unmark ()
  "Unmark the message at point."
  (interactive)
  (when-let* ((id (tabulated-list-get-id))) (rabbitmq-messages--set-mark id nil))
  (forward-line 1))

(defun rabbitmq-messages-unmark-all ()
  "Remove all marks."
  (interactive)
  (dolist (id rabbitmq-messages--order)
    (let ((m (gethash id rabbitmq-messages--table))) (puthash id (plist-put m :marked nil) rabbitmq-messages--table)))
  (rabbitmq-messages--render))

(defun rabbitmq-messages-mark-all-unsettled ()
  "Mark every unsettled message."
  (interactive)
  (dolist (id rabbitmq-messages--order)
    (let ((m (gethash id rabbitmq-messages--table)))
      (puthash id (plist-put m :marked (equal "unsettled" (rabbitmq-messages--state m))) rabbitmq-messages--table)))
  (rabbitmq-messages--render))

(defun rabbitmq-messages--record-settlement (id outcome)
  "Record OUTCOME for ID only while its message remains in the view."
  (when (gethash id rabbitmq-messages--table)
    (puthash id outcome rabbitmq-messages--settled)))

(defun rabbitmq-messages--settle (outcome label)
  (let* ((ids (rabbitmq-messages--marked))
         (buf (current-buffer))
         (by-kind (seq-group-by (lambda (id) (plist-get (plist-get (gethash id rabbitmq-messages--table) :source) :kind)) ids)))
    (when (assoc "stream" by-kind)
      (message "rabbitmq: stream messages are not settled; replay never removes them"))
    (when (> (length ids) 1)
      (unless (y-or-n-p (format "%s %d message(s)? " label (length ids))) (user-error "Cancelled")))
    (when-let* ((ids091 (cdr (assoc "amqp091" by-kind))))
      (rabbitmq-call 'message.settle (list :ids (vconcat ids091) :outcome outcome)
                     :then (lambda (r)
                             (when (buffer-live-p buf)
                              (with-current-buffer buf
                               (dolist (s (rabbitmq--vec (plist-get r :settled)))
                                 (rabbitmq-messages--record-settlement (plist-get s :id) (if (eq (plist-get s :settled) t) label "stale")))
                               (dolist (e (rabbitmq--vec (plist-get r :errors)))
                                 (let ((code (plist-get (plist-get e :error) :code)))
                                   (rabbitmq-messages--record-settlement (plist-get e :id) (if (equal code "stale_delivery") "stale" (format "error:%s" code)))
                                   (message "rabbitmq: %s" (plist-get (plist-get e :error) :message))))
                               (rabbitmq-messages-unmark-all))))))
    (dolist (id (cdr (assoc "amqp10" by-kind)))
      (let* ((src (plist-get (gethash id rabbitmq-messages--table) :source)))
        (rabbitmq-call 'amqp10.settle (list :link (plist-get src :link) :delivery_id (plist-get src :delivery_id) :outcome outcome)
                       :then (lambda (_)
                               (when (buffer-live-p buf)
                                 (with-current-buffer buf
                                   (rabbitmq-messages--record-settlement id label)
                                   (rabbitmq-messages--render)))))))))

(defun rabbitmq-messages-ack ()
  "Acknowledge (remove from the queue) the marked or current message(s)."
  (interactive)
  (rabbitmq-messages--settle "ack" "acked"))

(defun rabbitmq-messages-nack ()
  "Negatively acknowledge without requeue: dead-letter if configured, else drop."
  (interactive)
  (rabbitmq-messages--settle '(:nack (:requeue :json-false)) "nacked"))

(defun rabbitmq-messages-requeue ()
  "Nack with requeue: the message returns to the queue marked redelivered."
  (interactive)
  (rabbitmq-messages--settle '(:nack (:requeue t)) "requeued"))

(defun rabbitmq-messages-reject ()
  "Reject without requeue."
  (interactive)
  (rabbitmq-messages--settle '(:reject (:requeue :json-false)) "rejected"))

(defun rabbitmq-messages-clear-settled ()
  "Remove settled messages from the view."
  (interactive)
  (setq rabbitmq-messages--order
        (cl-remove-if (lambda (id) (let ((s (rabbitmq-messages--state (gethash id rabbitmq-messages--table))))
                                     (unless (member s '("unsettled" "replayed"))
                                       (remhash id rabbitmq-messages--table)
                                       (remhash id rabbitmq-messages--settled)
                                       t)))
                      rabbitmq-messages--order))
  (rabbitmq-messages--render))

;;;; Detail view

(defun rabbitmq-messages--decode (payload)
  "Return payload bytes as a unibyte string."
  (let ((data (or (plist-get payload :data) "")))
    (pcase (plist-get payload :encoding)
      ("base64" (base64-decode-string data))
      ("hex" (apply #'unibyte-string (cl-loop for i from 0 below (length data) by 2 collect (string-to-number (substring data i (+ i 2)) 16))))
      (_ (encode-coding-string data 'utf-8)))))

(defun rabbitmq-messages--hexdump (bytes)
  (with-temp-buffer
    (cl-loop for off from 0 below (length bytes) by 16
             for chunk = (substring bytes off (min (length bytes) (+ off 16)))
             do (insert (format "%08x  %-48s  %s\n" off
                                (mapconcat (lambda (b) (format "%02x" b)) chunk " ")
                                (mapconcat (lambda (b) (if (and (>= b 32) (< b 127)) (string b) ".")) chunk ""))))
    (buffer-string)))

(defvar-local rabbitmq-message--current nil)
(defvar-local rabbitmq-message--view 'auto)

(defvar-keymap rabbitmq-message-mode-map
  "t" #'rabbitmq-message-view-text
  "j" #'rabbitmq-message-view-json
  "h" #'rabbitmq-message-view-hex
  "b" #'rabbitmq-message-view-base64
  "w" #'rabbitmq-messages-copy-payload
  "q" #'quit-window)

(define-derived-mode rabbitmq-message-mode special-mode "RMQ-Message"
  "One message: properties, headers, and payload.
\\{rabbitmq-message-mode-map}")

(defun rabbitmq-message--render ()
  (let* ((m rabbitmq-message--current)
         (p (plist-get m :payload))
         (props (plist-get m :properties))
         (src (plist-get m :source))
         (bytes (rabbitmq-messages--decode p))
         (inhibit-read-only t))
    (erase-buffer)
    (insert (propertize "Message " 'face 'bold) (plist-get m :id) "\n")
    (insert (format "Source:       %s %s\n" (plist-get src :kind) (rabbitmq-messages--owner src)))
    (when (plist-get src :queue) (insert (format "Queue:        %s\n" (plist-get src :queue))))
    (when (plist-get src :stream) (insert (format "Stream:       %s\n" (plist-get src :stream))))
    (insert (format "Exchange:     %S\nRouting key:  %S\nRedelivered:  %s\nReceived:     %s\n"
                    (plist-get m :exchange) (plist-get m :routing_key)
                    (rabbitmq--str (plist-get m :redelivered))
                    (format-time-string "%F %T" (/ (plist-get m :received_at) 1000.0))))
    (insert (format "Payload:      %s bytes, %s%s\n\n" (plist-get p :size) (plist-get p :encoding)
                    (if (eq (plist-get p :truncated) t) (propertize " (TRUNCATED preview)" 'face 'warning) "")))
    (insert (propertize "Properties\n" 'face 'bold))
    (cl-loop for (k v) on props by #'cddr
             unless (memq k '(:headers :annotations))
             do (insert (format "  %-18s %s\n" (substring (symbol-name k) 1) (rabbitmq--str v))))
    (dolist (sect '((:headers . "Headers") (:annotations . "Annotations")))
      (when-let* ((h (plist-get props (car sect))))
        (insert (propertize (concat (cdr sect) "\n") 'face 'bold))
        (cl-loop for (k v) on h by #'cddr
                 do (insert (format "  %-18s %s\n" (substring (symbol-name k) 1)
                                    (if (listp v) (json-serialize v :null-object nil :false-object :json-false) (rabbitmq--str v)))))))
    (insert "\n" (propertize (format "Payload (%s view; t text, j json, h hex, b base64)\n" rabbitmq-message--view) 'face 'bold))
    (let ((start (point)))
      (pcase rabbitmq-message--view
        ('hex (insert (rabbitmq-messages--hexdump bytes)))
        ('base64 (insert (base64-encode-string bytes)))
        (_ (let ((text (decode-coding-string bytes 'utf-8)))
             (if (and (memq rabbitmq-message--view '(auto json))
                      (ignore-errors (ignore (json-parse-string text)) t))
                 (progn (insert text) (ignore-errors (json-pretty-print start (point))))
               (if (and (eq rabbitmq-message--view 'auto) (not (equal (plist-get p :encoding) "utf8")))
                   (insert (rabbitmq-messages--hexdump bytes))
                 (insert text)))))))
    (goto-char (point-min))))

(defun rabbitmq-message-view-text () "Show payload as text." (interactive) (setq rabbitmq-message--view 'text) (rabbitmq-message--render))
(defun rabbitmq-message-view-json () "Show payload as pretty JSON." (interactive) (setq rabbitmq-message--view 'json) (rabbitmq-message--render))
(defun rabbitmq-message-view-hex () "Show payload as a hex dump." (interactive) (setq rabbitmq-message--view 'hex) (rabbitmq-message--render))
(defun rabbitmq-message-view-base64 () "Show payload as base64." (interactive) (setq rabbitmq-message--view 'base64) (rabbitmq-message--render))

(defun rabbitmq-messages--at-point ()
  (or (gethash (or (tabulated-list-get-id) (user-error "No message at point")) rabbitmq-messages--table)
      (user-error "Message no longer in view")))

(defun rabbitmq-messages-show ()
  "Show the message at point in detail."
  (interactive)
  (let ((m (rabbitmq-messages--at-point)))
    (with-current-buffer (get-buffer-create "*rabbitmq-message*")
      (rabbitmq-message-mode)
      (setq rabbitmq-message--current m)
      (rabbitmq-message--render)
      (display-buffer (current-buffer)))))

(defun rabbitmq-messages-copy-payload ()
  "Copy the payload of the message at point (or the shown message)."
  (interactive)
  (let* ((m (if (derived-mode-p 'rabbitmq-message-mode) rabbitmq-message--current (rabbitmq-messages--at-point)))
         (p (plist-get m :payload)))
    (kill-new (if (equal (plist-get p :encoding) "utf8") (plist-get p :data) (plist-get p :data)))
    (message "Copied %s payload (%s)" (plist-get p :encoding) (if (eq (plist-get p :truncated) t) "truncated preview" "complete"))))

(defun rabbitmq-messages-export (file)
  "Export messages in view to FILE as JSON lines."
  (interactive "FExport messages to: ")
  (let ((messages (mapcar (lambda (id) (gethash id rabbitmq-messages--table))
                          rabbitmq-messages--order)))
    (with-temp-file file
      (dolist (m messages)
        (when m
          (insert (json-serialize m :null-object nil :false-object :json-false) "\n")))))
  (message "Exported %d message(s) to %s" (length rabbitmq-messages--order) file))

(defun rabbitmq-messages-diff ()
  "Diff the two marked messages (payload and properties)."
  (interactive)
  (let ((ids (cl-remove-if-not (lambda (id) (plist-get (gethash id rabbitmq-messages--table) :marked)) rabbitmq-messages--order)))
    (unless (= (length ids) 2) (user-error "Mark exactly two messages with m"))
    (let ((bufs (mapcar (lambda (id)
                          (let ((m (gethash id rabbitmq-messages--table)))
                            (with-current-buffer (get-buffer-create (format "*rabbitmq-diff %s*" id))
                              (erase-buffer)
                              (insert (json-serialize (list :properties (or (plist-get m :properties) (make-hash-table))
                                                            :routing_key (or (plist-get m :routing_key) "")
                                                            :payload (plist-get (plist-get m :payload) :data))
                                                      :null-object nil :false-object :json-false))
                              (json-pretty-print-buffer)
                              (current-buffer))))
                        ids)))
      (diff (car bufs) (cadr bufs) nil 'noasync))))

(declare-function rabbitmq-producer-from-message "rabbitmq-producer")
(defun rabbitmq-messages-republish ()
  "Open a producer buffer prefilled from the message at point."
  (interactive)
  (rabbitmq-producer-from-message (rabbitmq-messages--at-point)))

(provide 'rabbitmq-messages)
;;; rabbitmq-messages.el ends here
