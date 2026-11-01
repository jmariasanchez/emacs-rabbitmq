;;; rabbitmq-topology.el --- Exchanges, bindings, routing  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `rabbitmq-topology-mode' shows exchanges with their outgoing bindings
;; as an outline.  From here you can declare and delete exchanges, create
;; and remove bindings, predict routing (`r'), lint the topology for
;; unroutable exchanges and dangling bindings (`l'), render a Graphviz
;; graph (`G'), and diff against a definitions file or another session.

;;; Code:

(require 'rabbitmq-core)
(require 'outline)

(declare-function rabbitmq-show-json "rabbitmq-clusters")
(declare-function rabbitmq-show-route-prediction "rabbitmq-producer")
(declare-function rabbitmq-read-exchange "rabbitmq-queues")
(declare-function rabbitmq-read-queue "rabbitmq-queues")

(defvar-local rabbitmq-topology--snapshot nil)

(defvar-keymap rabbitmq-topology-mode-map
  "g" #'revert-buffer
  "r" #'rabbitmq-route
  "l" #'rabbitmq-topology-lint
  "G" #'rabbitmq-topology-graph
  "=" #'rabbitmq-topology-diff
  "+" #'rabbitmq-exchange-declare
  "D" #'rabbitmq-topology-delete-at-point
  "b" #'rabbitmq-binding-create
  "u" #'rabbitmq-topology-unbind-at-point
  "e" #'rabbitmq-definitions-export
  "TAB" #'outline-toggle-children
  "n" #'outline-next-visible-heading
  "p" #'outline-previous-visible-heading)

(define-derived-mode rabbitmq-topology-mode special-mode "RMQ-Topology"
  "Exchanges and bindings.

r predict routing   l lint   G graphviz   = diff   e export definitions
+ declare exchange   b bind   u unbind at point   D delete exchange at point
\\{rabbitmq-topology-mode-map}"
  (setq-local outline-regexp "^\\*+ ")
  (outline-minor-mode 1)
  (setq-local revert-buffer-function (lambda (&rest _) (rabbitmq-topology--refresh))))

(defun rabbitmq-topology--render (topo fetched)
  (let ((inhibit-read-only t)
        (exchanges (rabbitmq--vec (plist-get topo :exchanges)))
        (queues (rabbitmq--vec (plist-get topo :queues)))
        (bindings (rabbitmq--vec (plist-get topo :bindings))))
    (erase-buffer)
    (setq header-line-format (rabbitmq--header (format "Topology %s" (rabbitmq-session-vhost)) fetched))
    (insert (format "%d exchanges, %d queues, %d bindings\n\n" (length exchanges) (length queues) (length bindings)))
    (dolist (x (sort exchanges (lambda (a b) (string< (plist-get a :name) (plist-get b :name)))))
      (let* ((name (plist-get x :name))
             (out (cl-remove-if-not (lambda (b) (equal (plist-get b :source) name)) bindings))
             (args (plist-get x :arguments)))
        (insert (propertize (format "* %s" (if (string-empty-p name) "(default)" name)) 'face 'bold 'rabbitmq-exchange name)
                (propertize (format "  [%s%s%s%s]" (plist-get x :type)
                                    (if (eq (plist-get x :durable) t) " durable" "")
                                    (if (eq (plist-get x :internal) t) " internal" "")
                                    (if (eq (plist-get x :auto_delete) t) " auto-delete" ""))
                            'face 'font-lock-type-face 'rabbitmq-exchange name)
                (if-let* ((ae (plist-get args :alternate-exchange))) (propertize (format "  AE=%s" ae) 'face 'font-lock-keyword-face) "")
                (if-let* ((pol (plist-get x :policy))) (format "  policy=%s" pol) "")
                (if (and (null out) (not (string-empty-p name)) (not (string-prefix-p "amq." name)) (null (plist-get args :alternate-exchange)))
                    (propertize "  no bindings: unroutable" 'face 'rabbitmq-consume) "")
                "\n")
        (dolist (b out)
          (insert (propertize (format "    -> %-8s %-40s key=%S%s\n" (plist-get b :destination_type) (plist-get b :destination)
                                      (plist-get b :routing_key)
                                      (let ((a (plist-get b :arguments))) (if a (format " args=%s" (json-serialize a :null-object nil :false-object :json-false)) "")))
                              'rabbitmq-binding b)))))
    (insert "\n" (propertize "* Queues" 'face 'bold) "\n")
    (dolist (q (sort queues (lambda (a b) (string< (plist-get a :name) (plist-get b :name)))))
      (let ((dlx (plist-get (plist-get q :arguments) :x-dead-letter-exchange)))
        (insert (format "    %-40s [%s]%s\n" (plist-get q :name) (plist-get q :type)
                        (if dlx (propertize (format "  DLX=%s" dlx) 'face 'font-lock-keyword-face) "")))))
    (goto-char (point-min))))

(defun rabbitmq-topology--refresh ()
  (let ((buf (current-buffer)))
    (rabbitmq-call 'topology.snapshot (list :vhost (rabbitmq-session-vhost))
                   :then (lambda (r)
                           (when (buffer-live-p buf)
                             (with-current-buffer buf
                               (setq rabbitmq-topology--snapshot (plist-get r :data))
                               (rabbitmq-topology--render (plist-get r :data) (plist-get r :fetched_at))))))))

;;;###autoload
(defun rabbitmq-topology ()
  "Show exchanges, bindings and queues of the current session's vhost."
  (interactive)
  (rabbitmq-with-buffer (format "*rabbitmq-topology %s*" (rabbitmq-session)) 'rabbitmq-topology-mode
    (rabbitmq-topology--refresh)))

;;;###autoload
(defun rabbitmq-route (exchange routing-key &optional headers)
  "Predict where a message to EXCHANGE with ROUTING-KEY and HEADERS goes."
  (interactive (list (rabbitmq-read-exchange "Exchange: ") (read-string "Routing key: ")
                     (rabbitmq-read-json-object "Headers (JSON, optional): ")))
  (rabbitmq-call 'routing.simulate (list :exchange exchange :routing_key routing-key :headers (or headers (make-hash-table)))
                 :then #'rabbitmq-show-route-prediction))

(defun rabbitmq-topology-lint ()
  "Report dangling bindings and exchanges with no route."
  (interactive)
  (rabbitmq-call 'topology.lint (list :vhost (rabbitmq-session-vhost))
                 :then (lambda (r)
                         (let ((f (rabbitmq--vec (plist-get r :findings))))
                           (if (null f) (message "rabbitmq: no topology findings")
                             (with-current-buffer (get-buffer-create "*rabbitmq-lint*")
                               (let ((inhibit-read-only t))
                                 (erase-buffer)
                                 (insert (propertize "Topology findings\n\n" 'face 'bold))
                                 (dolist (x f) (insert "- " x "\n"))
                                 (special-mode))
                               (display-buffer (current-buffer))))))))

(defun rabbitmq-topology-graph ()
  "Render the topology with Graphviz (dot) or show the DOT source."
  (interactive)
  (rabbitmq-call 'topology.dot (list :vhost (rabbitmq-session-vhost))
                 :then (lambda (r)
                         (let ((dot (plist-get r :dot)))
                           (if (executable-find "dot")
                               (let ((png (make-temp-file "rabbitmq-topology" nil ".svg")))
                                 (with-temp-buffer
                                   (insert dot)
                                   (call-process-region (point-min) (point-max) "dot" nil nil nil "-Tsvg" "-o" png))
                                 (find-file-other-window png))
                             (with-current-buffer (get-buffer-create "*rabbitmq-topology.dot*")
                               (erase-buffer) (insert dot)
                               (when (fboundp 'graphviz-dot-mode) (graphviz-dot-mode))
                               (display-buffer (current-buffer))
                               (message "Install graphviz to render; showing DOT source")))))))

(defun rabbitmq-topology-diff (&optional file)
  "Diff the live topology with definitions in FILE or another session."
  (interactive (list (when (y-or-n-p "Compare with a definitions file? (n = another session) ")
                       (read-file-name "Definitions JSON: "))))
  (let ((params (if file
                    (list :vhost (rabbitmq-session-vhost)
                          :definitions (with-temp-buffer (insert-file-contents file)
                                                         (json-parse-buffer :object-type 'plist :null-object nil :false-object :json-false)))
                  (list :vhost (rabbitmq-session-vhost)
                        :other_session (completing-read "Other session: " (remove (rabbitmq-session) (rabbitmq-session-names)) nil t)))))
    (rabbitmq-call 'topology.diff params
                   :then (lambda (d)
                           (with-current-buffer (get-buffer-create "*rabbitmq-topology-diff*")
                             (let ((inhibit-read-only t))
                               (erase-buffer)
                               (insert (propertize (format "Live (%s) vs %s\n\n" (rabbitmq-session) (or file (plist-get params :other_session))) 'face 'bold))
                               (dolist (sect '((:removed "Only in live" diff-removed) (:added "Only in other" diff-added) (:changed "Different" diff-changed)))
                                 (let ((items (rabbitmq--vec (plist-get d (car sect)))))
                                   (insert (propertize (format "%s (%d)\n" (nth 1 sect) (length items)) 'face 'bold))
                                   (dolist (i items) (insert (propertize (concat "  " i "\n") 'face (nth 2 sect))))
                                   (insert "\n")))
                               (special-mode))
                             (display-buffer (current-buffer)))))))

(defun rabbitmq-exchange-declare (name type &optional durable internal arguments)
  "Declare exchange NAME of TYPE.  DURABLE, INTERNAL and ARGUMENTS as in AMQP.
Declaring an existing exchange with different settings fails with
precondition_failed and never alters the existing definition."
  (interactive (list (read-string "Exchange name: ")
                     (completing-read "Type: " '("direct" "topic" "fanout" "headers" "x-consistent-hash" "x-delayed-message") nil nil nil nil "topic")
                     (y-or-n-p "Durable? ") (y-or-n-p "Internal? ")
                     (rabbitmq-read-json-object "Arguments (JSON, e.g. {\"alternate-exchange\": \"ae\"}): ")))
  (rabbitmq-call 'exchange.declare (append (list :name name :type type :durable (if durable t :json-false) :internal (if internal t :json-false))
                                           (when arguments (list :arguments arguments)))
                 :then (lambda (_) (message "rabbitmq: declared %s exchange %s" type name) (rabbitmq--revert-queue-buffers))))

(defun rabbitmq-exchange-delete (name &optional if-unused)
  "Delete exchange NAME (typed confirmation)."
  (interactive (list (rabbitmq-read-exchange "Delete exchange: ") (y-or-n-p "Only if unused? ")))
  (when (or (string-empty-p name) (string-prefix-p "amq." name))
    (user-error "Built-in exchanges cannot be deleted"))
  (rabbitmq-confirm-destructive "exchange" name "removes the exchange and all its bindings")
  (rabbitmq-call 'exchange.delete (list :name name :confirm name :if_unused (if if-unused t :json-false))
                 :then (lambda (_) (message "rabbitmq: deleted exchange %s" name) (rabbitmq--revert-queue-buffers))))

(defun rabbitmq-binding-create (source destination-type destination routing-key &optional arguments)
  "Bind DESTINATION (a DESTINATION-TYPE) to exchange SOURCE with ROUTING-KEY."
  (interactive
   (let* ((src (rabbitmq-read-exchange "Source exchange: "))
          (dt (completing-read "Destination type: " '("queue" "exchange") nil t nil nil "queue")))
     (list src dt (if (equal dt "queue") (rabbitmq-read-queue "Queue: ") (rabbitmq-read-exchange "Destination exchange: "))
           (read-string "Routing key / pattern: ")
           (rabbitmq-read-json-object "Binding arguments (JSON, for headers exchanges): "))))
  (rabbitmq-call 'binding.create (append (list :source source :destination_type destination-type :destination destination :routing_key routing-key)
                                         (when arguments (list :arguments arguments)))
                 :then (lambda (_) (message "rabbitmq: bound %s %s to %s with %S" destination-type destination source routing-key)
                         (rabbitmq--revert-queue-buffers))))

(defun rabbitmq-topology-unbind-at-point ()
  "Remove the binding on the current line (typed confirmation)."
  (interactive)
  (let ((b (get-text-property (point) 'rabbitmq-binding)))
    (unless b (user-error "No binding on this line"))
    (rabbitmq-confirm-destructive "binding destination" (plist-get b :destination)
                                  (format "unbind %s from %s key %S" (plist-get b :destination) (plist-get b :source) (plist-get b :routing_key)))
    (rabbitmq-call 'binding.delete (list :source (plist-get b :source) :destination_type (plist-get b :destination_type)
                                         :destination (plist-get b :destination) :routing_key (plist-get b :routing_key)
                                         :arguments (or (plist-get b :arguments) (make-hash-table))
                                         :confirm (plist-get b :destination))
                   :then (lambda (_) (message "rabbitmq: binding removed") (rabbitmq--revert-queue-buffers)))))

(defun rabbitmq-topology-delete-at-point ()
  "Delete the exchange on the current line."
  (interactive)
  (let ((x (get-text-property (point) 'rabbitmq-exchange)))
    (unless x (user-error "No exchange on this line"))
    (rabbitmq-exchange-delete x)))

(defun rabbitmq-definitions-export (file)
  "Export the session vhost's definitions to FILE (secrets redacted)."
  (interactive "FExport definitions to: ")
  (rabbitmq-call 'mgmt.definitions.export (list :vhost (rabbitmq-session-vhost))
                 :then (lambda (d)
                         (with-temp-file file
                           (insert (json-serialize d :null-object nil :false-object :json-false))
                           (json-pretty-print-buffer))
                         (message "rabbitmq: wrote %s (password hashes redacted)" file))))

(defun rabbitmq-definitions-import (file)
  "Import definitions from FILE into the session vhost (typed confirmation)."
  (interactive "fImport definitions from: ")
  (rabbitmq-confirm-destructive "definitions import into vhost" (rabbitmq-session-vhost) "existing objects with the same names may be overwritten")
  (let ((defs (with-temp-buffer (insert-file-contents file) (json-parse-buffer :object-type 'plist :null-object nil :false-object :json-false))))
    (rabbitmq-call 'mgmt.definitions.import (list :vhost (rabbitmq-session-vhost) :definitions defs :confirm (rabbitmq-session-vhost))
                   :then (lambda (_) (message "rabbitmq: imported %s" file) (rabbitmq--revert-queue-buffers)))))

(declare-function rabbitmq--revert-queue-buffers "rabbitmq-queues")

(provide 'rabbitmq-topology)
;;; rabbitmq-topology.el ends here
