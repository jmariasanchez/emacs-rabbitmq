;;; rabbitmq-admin.el --- Users, permissions, vhosts, policies, limits  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `rabbitmq-admin-mode' is a tabbed administration buffer backed by the
;; Management API.  Each view lists one resource kind; `+' creates, `D'
;; deletes or revokes with typed confirmation.  Password hashes are never
;; shown; passwords are read with `read-passwd' and sent once.

;;; Code:

(require 'rabbitmq-core)

(declare-function rabbitmq-show-json "rabbitmq-clusters")

(defvar-local rabbitmq-admin--view 'users)

(defconst rabbitmq-admin--views
  '((users . "Users") (permissions . "Permissions") (topic-permissions . "Topic permissions")
    (vhosts . "Virtual hosts") (policies . "Policies") (operator-policies . "Operator policies")
    (limits . "Vhost limits") (connections . "Connections") (feature-flags . "Feature flags")))

(defvar-keymap rabbitmq-admin-mode-map
  "g" #'revert-buffer
  "TAB" #'rabbitmq-admin-next-view
  "<backtab>" #'rabbitmq-admin-prev-view
  "v" #'rabbitmq-admin-switch-view
  "RET" #'rabbitmq-admin-inspect
  "+" #'rabbitmq-admin-create
  "D" #'rabbitmq-admin-delete)

(define-derived-mode rabbitmq-admin-mode tabulated-list-mode "RMQ-Admin"
  "Broker administration (Management API).
TAB/S-TAB or v switch view   RET inspect   + create/set
D delete/revoke (typed confirmation)
\\{rabbitmq-admin-mode-map}"
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'rabbitmq-admin--refresh nil t))

(defun rabbitmq-admin--set-format (cols)
  (setq tabulated-list-format (vconcat (mapcar (lambda (c) (list (car c) (cdr c) t)) cols)))
  (tabulated-list-init-header))

(defun rabbitmq-admin--fill (method params cols row-fn)
  (let ((buf (current-buffer)))
    (rabbitmq-admin--set-format cols)
    (rabbitmq-call method params
                   :then (lambda (r)
                           (when (buffer-live-p buf)
                             (with-current-buffer buf
                               (setq header-line-format
                                     (concat (rabbitmq--header (alist-get rabbitmq-admin--view rabbitmq-admin--views) (plist-get r :fetched_at))
                                             "   TAB next view"))
                               (setq tabulated-list-entries (delq nil (mapcar row-fn (rabbitmq--items r))))
                               (tabulated-list-print t)))))))

(defun rabbitmq-admin--refresh ()
  (pcase rabbitmq-admin--view
    ('users (rabbitmq-admin--fill 'mgmt.users nil '(("User" . 30) ("Tags" . 30) ("Hashing" . 0))
                                  (lambda (u) (list u (vector (plist-get u :name) (rabbitmq--str (plist-get u :tags)) (or (plist-get u :hashing_algorithm) ""))))))
    ('permissions (rabbitmq-admin--fill 'mgmt.permissions nil '(("User" . 28) ("Vhost" . 16) ("Configure" . 18) ("Write" . 18) ("Read" . 0))
                                        (lambda (p) (list p (vector (plist-get p :user) (plist-get p :vhost) (plist-get p :configure) (plist-get p :write) (plist-get p :read))))))
    ('topic-permissions (rabbitmq-admin--fill 'mgmt.topic_permissions nil '(("User" . 28) ("Vhost" . 16) ("Exchange" . 20) ("Write" . 18) ("Read" . 0))
                                              (lambda (p) (list p (vector (plist-get p :user) (plist-get p :vhost) (plist-get p :exchange) (plist-get p :write) (plist-get p :read))))))
    ('vhosts (rabbitmq-admin--fill 'mgmt.vhosts nil '(("Vhost" . 24) ("Messages" . 10) ("Default type" . 13) ("Description" . 0))
                                   (lambda (v) (list v (vector (plist-get v :name) (rabbitmq--num (plist-get v :messages)) (or (plist-get v :default_queue_type) "") (or (plist-get v :description) ""))))))
    ('policies (rabbitmq-admin--fill 'mgmt.policies nil '(("Name" . 24) ("Vhost" . 12) ("Pattern" . 24) ("Apply to" . 10) ("Prio" . 5) ("Definition" . 0))
                                     #'rabbitmq-admin--policy-row))
    ('operator-policies (rabbitmq-admin--fill 'mgmt.operator_policies nil '(("Name" . 24) ("Vhost" . 12) ("Pattern" . 24) ("Apply to" . 10) ("Prio" . 5) ("Definition" . 0))
                                              #'rabbitmq-admin--policy-row))
    ('limits (rabbitmq-admin--fill 'mgmt.vhost_limits nil '(("Vhost" . 24) ("Limits" . 0))
                                   (lambda (l) (list l (vector (plist-get l :vhost) (json-serialize (or (plist-get l :value) (make-hash-table)) :null-object nil :false-object :json-false))))))
    ('connections (rabbitmq-admin--fill 'mgmt.connections nil '(("Name" . 40) ("User" . 14) ("Vhost" . 10) ("Protocol" . 18) ("State" . 9) ("Ch" . 4) ("TLS" . 4) ("Client" . 0))
                                        (lambda (c) (list c (vector (plist-get c :name) (or (plist-get c :user) "") (or (plist-get c :vhost) "")
                                                                    (or (plist-get c :protocol) "") (or (plist-get c :state) "")
                                                                    (rabbitmq--str (plist-get c :channels)) (rabbitmq--str (plist-get c :ssl))
                                                                    (or (plist-get (plist-get c :client_properties) :connection_name) ""))))))
    ('feature-flags (rabbitmq-admin--fill 'mgmt.feature_flags nil '(("Flag" . 40) ("State" . 10) ("Stability" . 12) ("Description" . 0))
                                          (lambda (f) (list f (vector (plist-get f :name)
                                                                      (propertize (plist-get f :state) 'face (if (equal (plist-get f :state) "enabled") 'rabbitmq-observe 'rabbitmq-consume))
                                                                      (or (plist-get f :stability) "") (or (plist-get f :desc) ""))))))))

(defun rabbitmq-admin--policy-row (p)
  (list p (vector (plist-get p :name) (plist-get p :vhost) (plist-get p :pattern) (or (plist-get p :apply-to) "")
                  (rabbitmq--str (plist-get p :priority))
                  (json-serialize (or (plist-get p :definition) (make-hash-table)) :null-object nil :false-object :json-false))))

;;;###autoload
(defun rabbitmq-admin (&optional view)
  "Open the administration buffer, optionally on VIEW."
  (interactive)
  (rabbitmq-with-buffer (format "*rabbitmq-admin %s*" (rabbitmq-session)) 'rabbitmq-admin-mode
    (when view (setq rabbitmq-admin--view view))
    (rabbitmq-admin--refresh)))

(defun rabbitmq-admin-switch-view (view)
  "Switch to VIEW."
  (interactive (list (intern (completing-read "View: " (mapcar (lambda (v) (symbol-name (car v))) rabbitmq-admin--views) nil t))))
  (setq rabbitmq-admin--view view)
  (rabbitmq-admin--refresh))

(defun rabbitmq-admin--cycle (delta)
  (let* ((keys (mapcar #'car rabbitmq-admin--views))
         (i (cl-position rabbitmq-admin--view keys)))
    (rabbitmq-admin-switch-view (nth (mod (+ i delta) (length keys)) keys))))

(defun rabbitmq-admin-next-view () "Next view." (interactive) (rabbitmq-admin--cycle 1))
(defun rabbitmq-admin-prev-view () "Previous view." (interactive) (rabbitmq-admin--cycle -1))

(defun rabbitmq-admin--item () (or (tabulated-list-get-id) (user-error "Nothing at point")))

(defun rabbitmq-admin-inspect ()
  "Show the item at point as JSON."
  (interactive)
  (rabbitmq-show-json "*rabbitmq-admin-item*" (rabbitmq-admin--item)))

(defun rabbitmq-admin--done (what)
  (lambda (_) (message "rabbitmq: %s" what) (revert-buffer)))

(defun rabbitmq-admin-create ()
  "Create or set an item of the current view."
  (interactive)
  (pcase rabbitmq-admin--view
    ('users
     (let* ((name (read-string "User name: "))
            (pw (read-passwd (format "Password for %s (empty for passwordless): " name) t))
            (tags (completing-read-multiple "Tags: " '("administrator" "monitoring" "policymaker" "management" "impersonator"))))
       (rabbitmq-call 'mgmt.user.put (append (list :name name :tags (string-join tags ","))
                                             (unless (string-empty-p pw) (list :password pw)))
                      :then (rabbitmq-admin--done (format "user %s saved" name)))
       (clear-string pw)))
    ('permissions
     (let ((user (read-string "User: ")) (vhost (read-string "Vhost: " (rabbitmq-session-vhost))))
       (rabbitmq-call 'mgmt.permissions.set (list :user user :vhost vhost
                                                  :configure (read-string "Configure regex: " ".*")
                                                  :write (read-string "Write regex: " ".*")
                                                  :read (read-string "Read regex: " ".*"))
                      :then (rabbitmq-admin--done (format "permissions for %s on %s set" user vhost)))))
    ('topic-permissions
     (let ((user (read-string "User: ")))
       (rabbitmq-call 'mgmt.topic_permissions.set (list :user user :vhost (read-string "Vhost: " (rabbitmq-session-vhost))
                                                        :exchange (read-string "Exchange: " "amq.topic")
                                                        :write (read-string "Write regex: " ".*") :read (read-string "Read regex: " ".*"))
                      :then (rabbitmq-admin--done (format "topic permissions for %s set" user)))))
    ('vhosts
     (let ((name (read-string "Vhost name: ")))
       (rabbitmq-call 'mgmt.vhost.create (list :name name :description (read-string "Description: ")
                                               :default_queue_type (completing-read "Default queue type: " '("quorum" "classic" "stream") nil t nil nil "quorum"))
                      :then (rabbitmq-admin--done (format "vhost %s created" name)))))
    ((or 'policies 'operator-policies)
     (let ((name (read-string "Policy name: ")))
       (rabbitmq-call (if (eq rabbitmq-admin--view 'policies) 'mgmt.policy.put 'mgmt.operator_policy.put)
                      (list :name name :vhost (read-string "Vhost: " (rabbitmq-session-vhost))
                            :pattern (read-string "Pattern (regex): ")
                            :apply-to (completing-read "Apply to: " '("all" "queues" "exchanges" "classic_queues" "quorum_queues" "streams") nil t nil nil "queues")
                            :priority (read-number "Priority: " 0)
                            :definition (or (rabbitmq-read-json-object "Definition (JSON, e.g. {\"max-length\": 1000}): ") (user-error "Definition required")))
                      :then (rabbitmq-admin--done (format "policy %s saved" name)))))
    ('limits
     (let ((vhost (read-string "Vhost: " (rabbitmq-session-vhost))))
       (rabbitmq-call 'mgmt.vhost_limit.set (list :vhost vhost :limit (completing-read "Limit: " '("max-connections" "max-queues") nil t)
                                                  :value (read-number "Value (-1 = unlimited): "))
                      :then (rabbitmq-admin--done (format "limit set on %s" vhost)))))
    (_ (user-error "Nothing to create in this view"))))

(defun rabbitmq-admin-delete ()
  "Delete or revoke the item at point (typed confirmation)."
  (interactive)
  (let ((it (rabbitmq-admin--item)))
    (pcase rabbitmq-admin--view
      ('users (let ((n (plist-get it :name)))
                (rabbitmq-confirm-destructive "user" n "deletes the user and all their permissions")
                (rabbitmq-call 'mgmt.user.delete (list :name n :confirm n) :then (rabbitmq-admin--done (format "user %s deleted" n)))))
      ('permissions (let ((u (plist-get it :user)) (v (plist-get it :vhost)))
                      (rabbitmq-confirm-destructive "user" u (format "revokes all permissions on vhost %s; open connections lose access" v))
                      (rabbitmq-call 'mgmt.permissions.clear (list :user u :vhost v :confirm u) :then (rabbitmq-admin--done "permissions revoked"))))
      ('topic-permissions (let ((u (plist-get it :user)) (v (plist-get it :vhost)))
                            (rabbitmq-confirm-destructive "user" u (format "revokes topic permissions on %s" v))
                            (rabbitmq-call 'mgmt.topic_permissions.clear (list :user u :vhost v :confirm u) :then (rabbitmq-admin--done "topic permissions revoked"))))
      ('vhosts (let ((n (plist-get it :name)))
                 (rabbitmq-confirm-destructive "vhost" n "DELETES EVERY QUEUE, EXCHANGE, BINDING, AND MESSAGE IN IT")
                 (rabbitmq-call 'mgmt.vhost.delete (list :name n :confirm n) :then (rabbitmq-admin--done (format "vhost %s deleted" n)))))
      ((or 'policies 'operator-policies)
       (let ((n (plist-get it :name)))
         (rabbitmq-confirm-destructive "policy" n)
         (rabbitmq-call (if (eq rabbitmq-admin--view 'policies) 'mgmt.policy.delete 'mgmt.operator_policy.delete)
                        (list :name n :vhost (plist-get it :vhost) :confirm n) :then (rabbitmq-admin--done (format "policy %s deleted" n)))))
      ('limits (let ((v (plist-get it :vhost)) (l (completing-read "Clear limit: " '("max-connections" "max-queues") nil t)))
                 (rabbitmq-call 'mgmt.vhost_limit.clear (list :vhost v :limit l) :then (rabbitmq-admin--done "limit cleared"))))
      ('connections (let ((n (plist-get it :name)))
                      (rabbitmq-confirm-destructive "connection" n "force-closes the client connection; its unacked messages are requeued")
                      (rabbitmq-call 'mgmt.connection.close (list :name n :confirm n :reason (read-string "Reason: " "closed by operator"))
                                     :then (rabbitmq-admin--done "connection closed"))))
      (_ (user-error "Nothing to delete in this view")))))

(provide 'rabbitmq-admin)
;;; rabbitmq-admin.el ends here
