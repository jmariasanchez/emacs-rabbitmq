;;; rabbitmq-queues.el --- Queue inspection and administration  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `rabbitmq-queues-mode' lists queues from the Management API with depth,
;; ready/unacked counts, consumers, type, leader and policy.  Inspecting a
;; queue is an observation.  Fetching messages from a classic or quorum
;; queue asks for consent; stream queues open the non-destructive stream
;; browser instead.

;;; Code:

(require 'rabbitmq-core)

(declare-function rabbitmq-messages-get "rabbitmq-messages")
(declare-function rabbitmq-consumer-start "rabbitmq-consumer")
(declare-function rabbitmq-streams-browse "rabbitmq-streams")
(declare-function rabbitmq-producer "rabbitmq-producer")
(declare-function rabbitmq-show-json "rabbitmq-clusters")

(defvar-local rabbitmq-queues--filter nil "Regex name filter for the queue list.")
(defvar-local rabbitmq-queues--vhost nil "Vhost shown, or nil for the session vhost.")
(defvar-local rabbitmq-queues--data nil "Last fetched queue plists, keyed by name.")

(defvar-keymap rabbitmq-queues-mode-map
  "RET" #'rabbitmq-queues-inspect
  "g" #'revert-buffer
  "/" #'rabbitmq-queues-filter
  "v" #'rabbitmq-queues-set-vhost
  "+" #'rabbitmq-queue-declare
  "D" #'rabbitmq-queues-delete
  "P" #'rabbitmq-queues-purge
  "f" #'rabbitmq-queues-fetch
  "c" #'rabbitmq-queues-consume
  "s" #'rabbitmq-queues-stream
  "p" #'rabbitmq-queues-publish
  "b" #'rabbitmq-queues-bindings
  "w" #'rabbitmq-queues-copy-name)

(define-derived-mode rabbitmq-queues-mode tabulated-list-mode "RMQ-Queues"
  "Queues of a RabbitMQ virtual host.

Observation keys (never change broker state):
  RET inspect   b bindings   g refresh   / filter   v vhost
Interventions:
  f fetch (basic.get, asks consent)
  c consume (asks consent)
  s browse stream (non-destructive, stream queues only)
  p publish   + declare
  P purge (typed confirmation)   D delete (typed confirmation)

\\{rabbitmq-queues-mode-map}"
  (setq tabulated-list-format [("Name" 40 t) ("Type" 8 t) ("State" 8 t) ("Messages" 9 rabbitmq-queues--sort-3)
                               ("Ready" 8 rabbitmq-queues--sort-4) ("Unacked" 8 rabbitmq-queues--sort-5)
                               ("Consumers" 9 rabbitmq-queues--sort-6) ("Leader/Node" 22 t) ("Policy" 0 t)])
  (setq tabulated-list-padding 1)
  (setq tabulated-list-sort-key '("Name"))
  (add-hook 'tabulated-list-revert-hook #'rabbitmq-queues--refresh nil t)
  (tabulated-list-init-header)
  (rabbitmq-auto-refresh-mode-setup))

(defun rabbitmq-queues--key (entry col)
  (string-to-number (or (get-text-property 0 'rabbitmq-sort (aref (cadr entry) col)) "0")))

(defun rabbitmq-queues--sort-3 (a b) (< (rabbitmq-queues--key a 3) (rabbitmq-queues--key b 3)))
(defun rabbitmq-queues--sort-4 (a b) (< (rabbitmq-queues--key a 4) (rabbitmq-queues--key b 4)))
(defun rabbitmq-queues--sort-5 (a b) (< (rabbitmq-queues--key a 5) (rabbitmq-queues--key b 5)))
(defun rabbitmq-queues--sort-6 (a b) (< (rabbitmq-queues--key a 6) (rabbitmq-queues--key b 6)))

(defun rabbitmq-queues--cell (n)
  (propertize (rabbitmq--num n) 'rabbitmq-sort (if (numberp n) (number-to-string n) "0")))

(defun rabbitmq-queues--entry (q)
  (let* ((type (or (plist-get q :type) "classic"))
         (name (plist-get q :name)))
    (list name
          (vector (propertize name 'face (if (equal type "stream") 'rabbitmq-observe 'default))
                  type
                  (or (plist-get q :state) "")
                  (rabbitmq-queues--cell (plist-get q :messages))
                  (rabbitmq-queues--cell (plist-get q :messages_ready))
                  (rabbitmq-queues--cell (plist-get q :messages_unacknowledged))
                  (rabbitmq-queues--cell (plist-get q :consumers))
                  (or (plist-get q :leader) (plist-get q :node) "")
                  (or (plist-get q :policy) "")))))

(defun rabbitmq-queues--refresh ()
  (let ((buf (current-buffer)))
    (rabbitmq-call 'mgmt.queues
                   (append (list :vhost (or rabbitmq-queues--vhost (rabbitmq-session-vhost)))
                           (when rabbitmq-queues--filter (list :name rabbitmq-queues--filter :use_regex t))
                           (list :columns ["name" "type" "state" "messages" "messages_ready" "messages_unacknowledged"
                                           "consumers" "leader" "node" "policy" "arguments" "durable" "auto_delete" "exclusive"]))
                   :then (lambda (r)
                           (when (buffer-live-p buf)
                             (with-current-buffer buf
                               (let ((items (rabbitmq--items r)))
                                 (setq rabbitmq-queues--data (mapcar (lambda (q) (cons (plist-get q :name) q)) items))
                                 (setq rabbitmq--last-refresh (plist-get r :fetched_at))
                                 (setq tabulated-list-entries (mapcar #'rabbitmq-queues--entry items))
                                 (setq header-line-format
                                       (concat (rabbitmq--header (format "Queues in %s" (or rabbitmq-queues--vhost (rabbitmq-session-vhost))) rabbitmq--last-refresh)
                                               (if rabbitmq-queues--filter (format "  filter: %s" rabbitmq-queues--filter) "")
                                               (format "  %d queue(s)" (length items))))
                                 (tabulated-list-print t))))))))

;;;###autoload
(defun rabbitmq-queues ()
  "Browse queues in the current session."
  (interactive)
  (rabbitmq-with-buffer (format "*rabbitmq-queues %s*" (rabbitmq-session)) 'rabbitmq-queues-mode
    (rabbitmq-queues--refresh)))

(defun rabbitmq-queues--at-point ()
  (or (tabulated-list-get-id) (user-error "No queue at point")))

(defun rabbitmq-queues--type (name)
  (or (plist-get (alist-get name rabbitmq-queues--data nil nil #'equal) :type) "classic"))

(defun rabbitmq-queues-filter (regex)
  "Filter queues by REGEX (empty clears)."
  (interactive (list (read-string "Queue name regex: " rabbitmq-queues--filter)))
  (setq rabbitmq-queues--filter (unless (string-empty-p regex) regex))
  (revert-buffer))

(defun rabbitmq-queues-set-vhost (vhost)
  "Show queues of VHOST."
  (interactive (list (rabbitmq-read-vhost)))
  (setq rabbitmq-queues--vhost vhost)
  (revert-buffer))

(defun rabbitmq-read-vhost ()
  "Read a vhost name with completion from the Management API."
  (let* ((r (ignore-errors (rabbitmq-rpc-call-sync 'mgmt.vhosts (list :session (rabbitmq-session)) 10)))
         (names (mapcar (lambda (v) (plist-get v :name)) (rabbitmq--items r))))
    (completing-read "Vhost: " names nil nil nil nil (rabbitmq-session-vhost))))

(defun rabbitmq-read-queue (&optional prompt)
  "Read a queue name with completion."
  (let* ((r (ignore-errors (rabbitmq-rpc-call-sync 'mgmt.queues (list :session (rabbitmq-session) :vhost (rabbitmq-session-vhost) :columns ["name"]) 10)))
         (names (mapcar (lambda (q) (plist-get q :name)) (rabbitmq--items r))))
    (completing-read (or prompt "Queue: ") names nil nil)))

(defun rabbitmq-read-exchange (&optional prompt)
  "Read an exchange name with completion (\"\" is the default exchange)."
  (let* ((r (ignore-errors (rabbitmq-rpc-call-sync 'mgmt.exchanges (list :session (rabbitmq-session) :vhost (rabbitmq-session-vhost) :columns ["name"]) 10)))
         (names (mapcar (lambda (q) (let ((n (plist-get q :name))) (if (string-empty-p n) "(default)" n))) (rabbitmq--items r)))
         (choice (completing-read (or prompt "Exchange: ") names nil nil)))
    (if (equal choice "(default)") "" choice)))

(defun rabbitmq-queues-inspect ()
  "Show full details of the queue at point (observation)."
  (interactive)
  (let ((name (rabbitmq-queues--at-point)))
    (rabbitmq-call 'mgmt.queue (list :name name :vhost (or rabbitmq-queues--vhost (rabbitmq-session-vhost)))
                   :then (lambda (r) (rabbitmq-show-json (format "*rabbitmq-queue %s*" name) (plist-get r :data))))))

(defun rabbitmq-queues-bindings ()
  "Show bindings of the queue at point."
  (interactive)
  (let ((name (rabbitmq-queues--at-point)))
    (rabbitmq-call 'mgmt.queue_bindings (list :name name :vhost (or rabbitmq-queues--vhost (rabbitmq-session-vhost)))
                   :then (lambda (r) (rabbitmq-show-json (format "*rabbitmq-bindings %s*" name) r)))))

(defun rabbitmq-queues-copy-name ()
  "Copy the queue name at point."
  (interactive)
  (kill-new (rabbitmq-queues--at-point))
  (message "Copied %s" (current-kill 0)))

(defun rabbitmq-queues-fetch (count)
  "Fetch COUNT messages from the queue at point with basic.get (asks consent)."
  (interactive "p")
  (let ((name (rabbitmq-queues--at-point)))
    (if (equal (rabbitmq-queues--type name) "stream")
        (rabbitmq-streams-browse name)
      (rabbitmq-messages-get name count))))

(defun rabbitmq-queues-consume ()
  "Start an interactive consumer on the queue at point."
  (interactive)
  (rabbitmq-consumer-start (rabbitmq-queues--at-point)))

(defun rabbitmq-queues-stream ()
  "Browse the stream at point without consuming."
  (interactive)
  (let ((name (rabbitmq-queues--at-point)))
    (unless (equal (rabbitmq-queues--type name) "stream")
      (user-error "%s is a %s queue; only streams support non-destructive replay" name (rabbitmq-queues--type name)))
    (rabbitmq-streams-browse name)))

(defun rabbitmq-queues-publish ()
  "Compose a message to the queue at point via the default exchange."
  (interactive)
  (rabbitmq-producer "" (rabbitmq-queues--at-point)))

(defun rabbitmq-queues-purge ()
  "Purge the queue at point (typed confirmation)."
  (interactive)
  (let* ((name (rabbitmq-queues--at-point))
         (q (alist-get name rabbitmq-queues--data nil nil #'equal)))
    (rabbitmq-queue-purge name (plist-get q :messages_ready))))

(defun rabbitmq-queue-purge (name &optional ready)
  "Purge queue NAME after typed confirmation.  READY is the known depth."
  (interactive (list (rabbitmq-read-queue "Purge queue: ")))
  (rabbitmq-confirm-destructive "queue" name (format "removes %s ready message(s); unacked messages are not affected" (or ready "all")))
  (rabbitmq-call 'queue.purge (list :name name :confirm name)
                 :then (lambda (r)
                         (message "rabbitmq: purged %s message(s) from %s" (plist-get r :messages_purged) name)
                         (rabbitmq--revert-queue-buffers))))

(defun rabbitmq-queues-delete (&optional if-empty)
  "Delete the queue at point.  With prefix IF-EMPTY, only if empty."
  (interactive "P")
  (rabbitmq-queue-delete (rabbitmq-queues--at-point) if-empty))

(defun rabbitmq-queue-delete (name &optional if-empty if-unused)
  "Delete queue NAME after typed confirmation."
  (interactive (list (rabbitmq-read-queue "Delete queue: ") (y-or-n-p "Only if empty? ") (y-or-n-p "Only if unused? ")))
  (rabbitmq-confirm-destructive "queue" name (concat "deletes the queue and all its messages"
                                                     (if if-empty "; only if empty" "")
                                                     (if if-unused "; only if unused" "")))
  (rabbitmq-call 'queue.delete (list :name name :confirm name :if_empty (if if-empty t :json-false) :if_unused (if if-unused t :json-false))
                 :then (lambda (r)
                         (message "rabbitmq: deleted %s (%s message(s) removed)" name (plist-get r :messages_deleted))
                         (rabbitmq--revert-queue-buffers))))

(defun rabbitmq--revert-queue-buffers ()
  (dolist (b (buffer-list))
    (with-current-buffer b
      (when (derived-mode-p 'rabbitmq-queues-mode 'rabbitmq-topology-mode)
        (revert-buffer)))))

(defun rabbitmq-queue-declare (name type &optional arguments)
  "Declare queue NAME of TYPE (classic, quorum, stream) with ARGUMENTS.
Arguments are validated against the queue type before anything is sent."
  (interactive
   (list (read-string "Queue name: ")
         (completing-read "Type: " '("quorum" "classic" "stream") nil t nil nil "quorum")
         (rabbitmq-read-json-object "Arguments (JSON, e.g. {\"x-max-length\": 1000}): ")))
  (rabbitmq-call 'queue.declare (append (list :name name :type type) (when arguments (list :arguments arguments)))
                 :then (lambda (r)
                         (message "rabbitmq: declared %s queue %s%s" type (plist-get r :name)
                                  (let ((w (rabbitmq--vec (plist-get r :warnings))))
                                    (if w (concat " (warning: " (string-join w "; ") ")") "")))
                         (rabbitmq--revert-queue-buffers))))

(provide 'rabbitmq-queues)
;;; rabbitmq-queues.el ends here
