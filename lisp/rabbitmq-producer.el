;;; rabbitmq-producer.el --- Compose and publish messages  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `rabbitmq-producer-mode' is a composition buffer.  The header block
;; above the separator holds routing and properties in `Key: value' form;
;; everything below is the payload.  C-c C-c publishes with publisher
;; confirms and shows the classified outcome (confirmed, returned, nacked,
;; uncertain).  C-c C-s predicts routing first without publishing.
;;
;; `rabbitmq-publish-region' and `rabbitmq-publish-buffer' publish text
;; from any buffer.  Templates save and restore header blocks.

;;; Code:

(require 'rabbitmq-core)

(defcustom rabbitmq-producer-templates-file (locate-user-emacs-file "rabbitmq-templates.eld")
  "File where message templates are stored."
  :type 'file
  :group 'rabbitmq)

(defconst rabbitmq-producer--separator "--text follows this line--")

(defvar-keymap rabbitmq-producer-mode-map
  "C-c C-c" #'rabbitmq-producer-send
  "C-c C-s" #'rabbitmq-producer-simulate
  "C-c C-t" #'rabbitmq-producer-save-template
  "C-c C-l" #'rabbitmq-producer-load-template
  "C-c C-k" #'kill-current-buffer)

(define-derived-mode rabbitmq-producer-mode text-mode "RMQ-Produce"
  "Compose a RabbitMQ message.

Header keys (one per line, before the separator):
  Session, Protocol (amqp091 | stream | amqp10), Exchange, Routing-Key,
  Stream, Address, Mandatory (yes/no), Persistent (yes/no), Content-Type,
  Content-Encoding, Correlation-Id, Reply-To, Message-Id, Type, App-Id,
  Expiration (ms), Priority, Encoding (utf8 | base64 | hex), Count,
  Mode (confirm | transaction | no_confirm), Filter-Value, Producer-Name,
  Header-NAME: VALUE (repeatable; VALUE parsed as JSON when possible).

\\{rabbitmq-producer-mode-map}"
  (setq-local header-line-format
              (substitute-command-keys " RabbitMQ producer   \\[rabbitmq-producer-send] publish   \\[rabbitmq-producer-simulate] predict routing   \\[rabbitmq-producer-save-template] save template   \\[rabbitmq-producer-load-template] load template")))

(defun rabbitmq-producer--template (headers payload)
  (concat (mapconcat (lambda (h) (format "%s: %s" (car h) (cdr h))) headers "\n")
          "\n" rabbitmq-producer--separator "\n" payload))

;;;###autoload
(defun rabbitmq-producer (&optional exchange routing-key payload)
  "Open a composition buffer for EXCHANGE and ROUTING-KEY with PAYLOAD."
  (interactive)
  (let ((session (rabbitmq-session)))
    (with-current-buffer (generate-new-buffer "*rabbitmq-producer*")
      (rabbitmq-producer-mode)
      (setq rabbitmq-current-session session)
      (insert (rabbitmq-producer--template
               `(("Session" . ,session) ("Protocol" . "amqp091") ("Exchange" . ,(or exchange ""))
                 ("Routing-Key" . ,(or routing-key "")) ("Mandatory" . "yes") ("Persistent" . "yes")
                 ("Content-Type" . "application/json") ("Mode" . "confirm"))
               (or payload "{}\n")))
      (goto-char (point-min))
      (re-search-forward "^Routing-Key: " nil t)
      (pop-to-buffer (current-buffer)))))

(defun rabbitmq-producer-from-message (m)
  "Open a producer prefilled from inbound message plist M."
  (let* ((props (plist-get m :properties))
         (p (plist-get m :payload))
         (src (plist-get m :source))
         (headers (cl-loop for (k v) on (plist-get props :headers) by #'cddr
                           collect (cons (concat "Header-" (substring (symbol-name k) 1))
                                         (if (stringp v) v (json-serialize v :null-object nil :false-object :json-false))))))
    (when (eq (plist-get p :truncated) t)
      (message "rabbitmq: payload preview was truncated; the republished body will be the preview only"))
    (with-current-buffer (generate-new-buffer "*rabbitmq-producer*")
      (rabbitmq-producer-mode)
      (setq rabbitmq-current-session (rabbitmq-session))
      (insert (rabbitmq-producer--template
               (append `(("Session" . ,rabbitmq-current-session) ("Protocol" . "amqp091")
                         ("Exchange" . ,(or (plist-get m :exchange) ""))
                         ("Routing-Key" . ,(or (plist-get m :routing_key) (plist-get src :queue) ""))
                         ("Mandatory" . "yes")
                         ("Persistent" . ,(if (eql (plist-get props :delivery_mode) 1) "no" "yes"))
                         ("Encoding" . ,(plist-get p :encoding)))
                       (cl-loop for (k . h) in '((:content_type . "Content-Type") (:correlation_id . "Correlation-Id")
                                                 (:reply_to . "Reply-To") (:message_id . "Message-Id") (:type . "Type")
                                                 (:app_id . "App-Id") (:priority . "Priority") (:expiration . "Expiration"))
                                when (plist-get props k) collect (cons h (rabbitmq--str (plist-get props k))))
                       headers)
               (plist-get p :data)))
      (goto-char (point-min))
      (pop-to-buffer (current-buffer)))))

(defun rabbitmq-producer--parse ()
  "Parse the composition buffer into (HEADERS-ALIST . PAYLOAD)."
  (save-excursion
    (goto-char (point-min))
    (unless (re-search-forward (concat "^" (regexp-quote rabbitmq-producer--separator) "$") nil t)
      (user-error "Missing separator line %S" rabbitmq-producer--separator))
    (let ((payload (buffer-substring-no-properties (1+ (point)) (point-max)))
          (end (match-beginning 0))
          headers)
      (goto-char (point-min))
      (while (re-search-forward "^\\([A-Za-z][A-Za-z0-9_.-]*\\):[ \t]*\\(.*\\)$" end t)
        (push (cons (match-string-no-properties 1) (string-trim (match-string-no-properties 2))) headers))
      (cons (nreverse headers) payload))))

(defun rabbitmq-producer--h (headers key)
  (let ((v (cdr (assoc-string key headers t))))
    (and v (not (string-empty-p v)) v)))

(defun rabbitmq-producer--yes (v) (and v (member (downcase v) '("yes" "y" "true" "t" "1"))))

(defun rabbitmq-producer--json-or-string (s)
  (condition-case nil (json-parse-string s :object-type 'plist :null-object nil :false-object :json-false) (error s)))

(defun rabbitmq-producer--properties (h)
  (let ((props nil) (headers nil))
    (dolist (pair h)
      (when (string-prefix-p "header-" (downcase (car pair)))
        (setq headers (append headers (list (intern (concat ":" (substring (car pair) 7)))
                                            (rabbitmq-producer--json-or-string (cdr pair)))))))
    (cl-loop for (key . prop) in '(("Content-Type" . :content_type) ("Content-Encoding" . :content_encoding)
                                   ("Correlation-Id" . :correlation_id) ("Reply-To" . :reply_to)
                                   ("Message-Id" . :message_id) ("Type" . :type) ("App-Id" . :app_id)
                                   ("Expiration" . :expiration) ("User-Id" . :user_id))
             for v = (rabbitmq-producer--h h key)
             when v do (setq props (plist-put props prop v)))
    (when-let* ((pr (rabbitmq-producer--h h "Priority"))) (setq props (plist-put props :priority (string-to-number pr))))
    (setq props (plist-put props :delivery_mode (if (equal (downcase (or (rabbitmq-producer--h h "Persistent") "yes")) "no") 1 2)))
    (setq props (plist-put props :timestamp (truncate (float-time))))
    (when headers (setq props (plist-put props :headers headers)))
    props))

(defun rabbitmq-producer--payload (h body)
  (let ((enc (or (rabbitmq-producer--h h "Encoding") "utf8")))
    (list :encoding enc :data (if (equal enc "utf8") body (string-trim body)))))

(defun rabbitmq-producer--report (r what)
  (let* ((s (plist-get r :summary))
         (outcomes (rabbitmq--vec (plist-get r :outcomes)))
         (bad (cl-remove-if (lambda (o) (member (plist-get o :status) '("confirmed" "committed"))) outcomes)))
    (message "rabbitmq: %s: %s"
             what
             (mapconcat #'identity
                        (delq nil (list (and (> (plist-get s :confirmed) 0) (propertize (format "%d confirmed" (plist-get s :confirmed)) 'face 'rabbitmq-observe))
                                        (and (> (plist-get s :committed) 0) (format "%d committed" (plist-get s :committed)))
                                        (and (> (plist-get s :returned) 0) (propertize (format "%d RETURNED (unroutable)" (plist-get s :returned)) 'face 'rabbitmq-destructive))
                                        (and (> (plist-get s :nacked) 0) (propertize (format "%d NACKED" (plist-get s :nacked)) 'face 'rabbitmq-destructive))
                                        (and (> (plist-get s :uncertain) 0) (propertize (format "%d UNCERTAIN (broker may or may not have them; not retried)" (plist-get s :uncertain)) 'face 'rabbitmq-uncertain))
                                        (and (> (plist-get s :unconfirmed) 0) (format "%d sent without confirms" (plist-get s :unconfirmed)))
                                        (and (> (plist-get s :failed) 0) (propertize (format "%d failed" (plist-get s :failed)) 'face 'rabbitmq-destructive))))
                        ", "))
    (when bad
      (rabbitmq-events-log "warning" "publish"
                           (mapconcat (lambda (o) (format "#%s %s %s" (plist-get o :index) (plist-get o :status)
                                                          (or (plist-get o :reply_text) (plist-get o :error) "")))
                                      bad "; ")))))

(declare-function rabbitmq-events-log "rabbitmq-events")

(defun rabbitmq-producer-send ()
  "Publish the composed message and report the classified outcome."
  (interactive)
  (pcase-let* ((`(,h . ,body) (rabbitmq-producer--parse))
               (session (or (rabbitmq-producer--h h "Session") (rabbitmq-session)))
               (protocol (or (rabbitmq-producer--h h "Protocol") "amqp091"))
               (count (max 1 (string-to-number (or (rabbitmq-producer--h h "Count") "1"))))
               (payload (rabbitmq-producer--payload h body))
               (props (rabbitmq-producer--properties h))
               (rabbitmq-current-session session))
    (pcase protocol
      ("amqp091"
       (let ((msg (list :exchange (or (rabbitmq-producer--h h "Exchange") "")
                        :routing_key (or (rabbitmq-producer--h h "Routing-Key") "")
                        :mandatory (if (rabbitmq-producer--yes (or (rabbitmq-producer--h h "Mandatory") "yes")) t :json-false)
                        :payload payload :properties props)))
         (rabbitmq-call 'message.publish (list :messages (make-vector count msg)
                                               :mode (or (rabbitmq-producer--h h "Mode") "confirm"))
                        :then (lambda (r) (rabbitmq-producer--report r "publish")))))
      ("stream"
       (let* ((stream (or (rabbitmq-producer--h h "Stream") (rabbitmq-producer--h h "Routing-Key") (user-error "Stream: header required")))
              (m (append (list :payload payload :properties props)
                         (when-let* ((f (rabbitmq-producer--h h "Filter-Value"))) (list :filter_value f)))))
         (rabbitmq-call 'stream.publish (append (list :stream stream :messages (make-vector count m))
                                                (when-let* ((n (rabbitmq-producer--h h "Producer-Name"))) (list :producer_name n)))
                        :then (lambda (r) (rabbitmq-producer--report r (format "stream %s" stream))))))
      ("amqp10"
       (let* ((addr (or (rabbitmq-producer--h h "Address") (user-error "Address: header required (e.g. /queues/q)")))
              (m (list :payload payload :properties props)))
         (rabbitmq-call 'amqp10.sender.open (list :address (list :type "raw" :address addr))
                        :then (lambda (link)
                                (let ((id (plist-get link :id)))
                                  (rabbitmq-call 'amqp10.send (list :link id :messages (make-vector count m))
                                                 :then (lambda (r)
                                                         (rabbitmq-producer--report r (format "AMQP 1.0 %s" addr))
                                                         (rabbitmq-call 'amqp10.sender.close (list :link id)))))))))
      (other (user-error "Unknown protocol %s" other)))))

(defun rabbitmq-producer-simulate ()
  "Predict where the composed message would be routed (no publish)."
  (interactive)
  (pcase-let* ((`(,h . ,_) (rabbitmq-producer--parse))
               (rabbitmq-current-session (or (rabbitmq-producer--h h "Session") (rabbitmq-session)))
               (props (rabbitmq-producer--properties h)))
    (rabbitmq-call 'routing.simulate (list :exchange (or (rabbitmq-producer--h h "Exchange") "")
                                           :routing_key (or (rabbitmq-producer--h h "Routing-Key") "")
                                           :headers (or (plist-get props :headers) (make-hash-table)))
                   :then #'rabbitmq-show-route-prediction)))

(defun rabbitmq-show-route-prediction (p)
  "Display routing prediction P."
  (with-current-buffer (get-buffer-create "*rabbitmq-route*")
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert (propertize "Routing PREDICTION" 'face 'bold) " (from known bindings; not a delivery confirmation)\n\n")
      (if (eq (plist-get p :unroutable) t)
          (insert (propertize "UNROUTABLE: " 'face 'rabbitmq-destructive)
                  "no queue matches.  With mandatory the broker returns the message; otherwise it is dropped.\n")
        (insert "Queues: " (propertize (string-join (rabbitmq--vec (plist-get p :queues)) ", ") 'face 'rabbitmq-observe) "\n"))
      (when-let* ((ae (rabbitmq--vec (plist-get p :alternate_exchanges_used))))
        (insert "Alternate exchanges used: " (string-join ae ", ") "\n"))
      (insert "\nHops:\n")
      (dolist (h (rabbitmq--vec (plist-get p :hops)))
        (insert (format "  %s -> %s %s   (%s)\n" (let ((x (plist-get h :exchange))) (if (string-empty-p x) "(default)" x))
                        (plist-get h :destination_type) (plist-get h :destination) (plist-get h :reason))))
      (insert "\nCaveats:\n")
      (dolist (c (rabbitmq--vec (plist-get p :caveats))) (insert "  - " c "\n"))
      (special-mode))
    (display-buffer (current-buffer))))

;;;###autoload
(defun rabbitmq-publish-region (beg end exchange routing-key)
  "Publish the region BEG..END to EXCHANGE with ROUTING-KEY (with confirms)."
  (interactive (append (if (use-region-p) (list (region-beginning) (region-end)) (list (point-min) (point-max)))
                       (list (rabbitmq-read-exchange "Exchange: ") (read-string "Routing key: "))))
  (let ((text (buffer-substring-no-properties beg end)))
    (rabbitmq-call 'message.publish
                   (list :messages (vector (list :exchange exchange :routing_key routing-key :mandatory t
                                                 :payload (list :encoding "utf8" :data text)
                                                 :properties (list :delivery_mode 2 :timestamp (truncate (float-time))
                                                                   :content_type (if (ignore-errors (ignore (json-parse-string text)) t) "application/json" "text/plain")))))
                   :then (lambda (r) (rabbitmq-producer--report r "publish region")))))

;;;###autoload
(defun rabbitmq-publish-buffer (exchange routing-key)
  "Publish the whole buffer to EXCHANGE with ROUTING-KEY."
  (interactive (list (rabbitmq-read-exchange "Exchange: ") (read-string "Routing key: ")))
  (rabbitmq-publish-region (point-min) (point-max) exchange routing-key))

;;;; Templates

(defun rabbitmq-producer--templates ()
  (when (file-exists-p rabbitmq-producer-templates-file)
    (with-temp-buffer
      (insert-file-contents rabbitmq-producer-templates-file)
      (ignore-errors (read (current-buffer))))))

(defun rabbitmq-producer-save-template (name)
  "Save the current composition as template NAME."
  (interactive "sTemplate name: ")
  (let ((all (cl-remove name (rabbitmq-producer--templates) :key #'car :test #'equal)))
    (push (cons name (buffer-substring-no-properties (point-min) (point-max))) all)
    (with-temp-file rabbitmq-producer-templates-file
      (let ((print-length nil) (print-level nil)) (prin1 all (current-buffer))))
    (message "Saved template %s" name)))

(defun rabbitmq-producer-load-template (name)
  "Replace the composition with template NAME."
  (interactive (list (completing-read "Template: " (mapcar #'car (rabbitmq-producer--templates)) nil t)))
  (erase-buffer)
  (insert (cdr (assoc name (rabbitmq-producer--templates))))
  (goto-char (point-min)))

(declare-function rabbitmq-read-exchange "rabbitmq-queues")

(provide 'rabbitmq-producer)
;;; rabbitmq-producer.el ends here
