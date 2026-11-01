;;; rabbitmq-test.el --- ERT tests for rabbitmq.el  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Unit tests run anywhere.  Integration tests (tagged :integration) start
;; the real rabbitmq-bridge and talk to the isolated test broker; they are
;; skipped when no broker listens on localhost:5672.
;;
;; Run: make test-elisp  (or see tests/ert/run.sh)

;;; Code:

(require 'ert)
(require 'cl-lib)
(require 'rabbitmq)

;;;; Helpers

(defun rabbitmq-test--broker-p ()
  (condition-case nil
      (let ((p (make-network-process :name "rmq-probe" :host "127.0.0.1" :service 5672 :nowait nil)))
        (delete-process p) t)
    (error nil)))

(defmacro rabbitmq-test--with-bridge (&rest body)
  "Run BODY with a fresh bridge and a session named \"ert\"."
  (declare (indent 0))
  `(progn
     (skip-unless (rabbitmq-test--broker-p))
     (let ((rabbitmq-profiles '(("ert" :host "localhost" :management-url "http://localhost:15672"
                                       :username "guest" :password "guest")))
           (rabbitmq--sessions (make-hash-table :test #'equal))
           (rabbitmq--last-session nil))
       (unwind-protect
           (progn
             (rabbitmq-rpc-stop)
             (rabbitmq-rpc-call-sync 'session.open (list :session "ert" :profile (rabbitmq-profile-json "ert")) 60)
             (puthash "ert" (rabbitmq-rpc-call-sync 'session.info '(:session "ert")) rabbitmq--sessions)
             (setq rabbitmq--last-session "ert")
             (let ((rabbitmq-current-session "ert"))
               ,@body))
         (rabbitmq-rpc-stop)))))

(defun rabbitmq-test--wait (pred &optional timeout)
  "Spin the event loop until PRED returns non-nil or TIMEOUT seconds pass."
  (let ((deadline (+ (float-time) (or timeout 10))))
    (while (and (not (funcall pred)) (< (float-time) deadline))
      (accept-process-output nil 0.05))
    (funcall pred)))

(defun rabbitmq-test--name (prefix)
  (format "rmqel-test.ert.%s.%s.%d" prefix (emacs-pid) (random 1000000)))

(defun rabbitmq-test--call (method params)
  (rabbitmq-rpc-call-sync method (append (list :session "ert") params) 30))

(defun rabbitmq-test--error-code (method params)
  "Return the bridge error code string for METHOD, or nil if it succeeded."
  (condition-case err
      (progn (rabbitmq-test--call method params) nil)
    (rabbitmq-error (nth 2 err))))

;;;; Unit tests

(ert-deftest rabbitmq-test-plist-to-json ()
  (should (equal (rabbitmq--plist-to-json '(:management-url "u" :read-only t :tls (:ca-file "c" :verify t)))
                 '(:management_url "u" :read_only t :tls (:ca_file "c" :verify t))))
  (should (equal (rabbitmq--plist-to-json '(:auth-mechanism external)) '(:auth_mechanism "external")))
  (let ((j (rabbitmq--plist-to-json '(:protected ((:kind "queue" :name "prod.*"))))))
    (should (vectorp (plist-get j :protected)))
    (should (equal (aref (plist-get j :protected) 0) '(:kind "queue" :name "prod.*")))))

(ert-deftest rabbitmq-test-profile-json-defaults ()
  (let ((rabbitmq-profiles '(("p" :host "h" :password "s3cret"))))
    (let ((j (rabbitmq-profile-json "p")))
      (should (equal (plist-get j :name) "p"))
      (should (equal (plist-get j :host) "h"))
      (should (eq (plist-get j :read_only) :json-false))
      (should (equal (plist-get j :password) "s3cret"))))
  (should-error (rabbitmq-profile-json "nope") :type 'user-error))

(ert-deftest rabbitmq-test-auth-source-password ()
  (let* ((rabbitmq-profiles '(("p" :host "mq.example" :username "alice")))
         (auth-sources nil)
         (auth-source-do-cache nil))
    (cl-letf (((symbol-function 'auth-source-search)
               (lambda (&rest spec)
                 (when (and (equal (plist-get spec :host) "mq.example") (equal (plist-get spec :user) "alice"))
                   (list (list :secret (lambda () "from-auth-source")))))))
      (should (equal (plist-get (rabbitmq-profile-json "p") :password) "from-auth-source")))))

(ert-deftest rabbitmq-test-formatting ()
  (should (equal (rabbitmq--num 12) "12"))
  (should (equal (rabbitmq--num 25000) "25.0k"))
  (should (equal (rabbitmq--num 3500000) "3.5M"))
  (should (equal (substring-no-properties (rabbitmq--num nil)) "?"))
  (should (equal (rabbitmq--str :json-false) "no"))
  (should (equal (rabbitmq--str t) "yes"))
  (should (equal (rabbitmq--str ["a" "b"]) "a,b"))
  (should (equal (rabbitmq--items '(:fetched_at 1 :data (:items [(:a 1)] :page 1))) '((:a 1))))
  (should (equal (rabbitmq--items '(:fetched_at 1 :data [(:a 1) (:a 2)])) '((:a 1) (:a 2)))))

(ert-deftest rabbitmq-test-confirm-destructive ()
  (let ((rabbitmq--sessions (make-hash-table :test #'equal))
        (rabbitmq-current-session "s"))
    (puthash "s" '(:read_only :json-false) rabbitmq--sessions)
    (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "orders")))
      (should (equal (rabbitmq-confirm-destructive "queue" "orders") "orders")))
    (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "order")))
      (should-error (rabbitmq-confirm-destructive "queue" "orders") :type 'user-error))
    (puthash "s" '(:read_only t) rabbitmq--sessions)
    (should-error (rabbitmq-confirm-destructive "queue" "orders") :type 'user-error)
    (should-error (rabbitmq-consent-consume "orders" "Fetching") :type 'user-error)))

(ert-deftest rabbitmq-test-producer-parse ()
  (with-temp-buffer
    (rabbitmq-producer-mode)
    (insert "Session: s\nExchange: x\nRouting-Key: a.b\nPersistent: no\nPriority: 3\nHeader-tenant: \"acme\"\nHeader-n: 5\n"
            rabbitmq-producer--separator "\n{\"k\": 1}\n")
    (pcase-let ((`(,h . ,body) (rabbitmq-producer--parse)))
      (should (equal (rabbitmq-producer--h h "Exchange") "x"))
      (should (equal (rabbitmq-producer--h h "routing-key") "a.b"))
      (should (equal body "{\"k\": 1}\n"))
      (let ((props (rabbitmq-producer--properties h)))
        (should (eql (plist-get props :delivery_mode) 1))
        (should (eql (plist-get props :priority) 3))
        (should (equal (plist-get (plist-get props :headers) :tenant) "acme"))
        (should (eql (plist-get (plist-get props :headers) :n) 5))))))

(ert-deftest rabbitmq-test-hexdump-and-decode ()
  (let ((bytes (rabbitmq-messages--decode '(:encoding "base64" :data "/wBB"))))
    (should (equal (append bytes nil) '(255 0 65)))
    (should (string-match-p "^00000000  ff 00 41 .* \\.\\.A$" (string-trim-right (rabbitmq-messages--hexdump bytes)))))
  (should (equal (append (rabbitmq-messages--decode '(:encoding "hex" :data "0a0b")) nil) '(10 11))))

(ert-deftest rabbitmq-test-messages-buffer-bounded ()
  (let ((rabbitmq--sessions (make-hash-table :test #'equal))
        (rabbitmq--last-session "s")
        (rabbitmq-messages-max 3))
    (puthash "s" '(:read_only :json-false) rabbitmq--sessions)
    (let ((buf (rabbitmq-messages-buffer "t" '(:kind get))))
      (unwind-protect
          (progn
            (rabbitmq-messages-add buf (vconcat (cl-loop for i below 5 collect
                                                         (list :id (format "m%d" i)
                                                               :source '(:kind "amqp091" :channel 1 :delivery_tag 1 :connection_epoch 1 :auto_ack :json-false)
                                                               :payload '(:encoding "utf8" :data "x" :size 1 :truncated :json-false)
                                                               :received_at 0))))
            (with-current-buffer buf
              (should (= (length rabbitmq-messages--order) 3))
              (should (= rabbitmq-messages--dropped 2))
              (should (equal rabbitmq-messages--order '("m2" "m3" "m4")))
              (should (equal (rabbitmq-messages--state (gethash "m4" rabbitmq-messages--table)) "unsettled"))))
        (kill-buffer buf)))))

(ert-deftest rabbitmq-test-find-bridge ()
  (let ((rabbitmq-bridge-program nil))
    (should (file-executable-p (rabbitmq-rpc-find-bridge)))))

;;;; Integration tests

(ert-deftest rabbitmq-test-integration-handshake ()
  :tags '(:integration)
  (rabbitmq-rpc-stop)
  (rabbitmq-rpc-start)
  (should (eql (plist-get rabbitmq-rpc--hello :protocol_version) rabbitmq-rpc-protocol-version))
  (should (eq t (plist-get (rabbitmq-rpc-call-sync 'bridge.ping nil) :pong)))
  (rabbitmq-rpc-stop)
  (should-not (rabbitmq-rpc-live-p)))

(ert-deftest rabbitmq-test-integration-publish-get-settle ()
  :tags '(:integration)
  (rabbitmq-test--with-bridge
    (let ((q (rabbitmq-test--name "q")))
      (rabbitmq-test--call 'queue.declare (list :name q :type "quorum"))
      (let ((r (rabbitmq-test--call 'message.publish
                                    (list :messages (vector (list :routing_key q :payload '(:data "from emacs") :mandatory t))))))
        (should (eql (plist-get (plist-get r :summary) :confirmed) 1)))
      ;; Consent is enforced by the bridge, not only by the UI.
      (should (equal (rabbitmq-test--error-code 'message.get (list :queue q)) "confirmation_required"))
      (let* ((got (rabbitmq-test--call 'message.get (list :queue q :consent t)))
             (m (aref (plist-get got :messages) 0)))
        (should (eq (plist-get got :destructive) t))
        (should (equal (plist-get (plist-get m :payload) :data) "from emacs"))
        (let ((s (rabbitmq-test--call 'message.settle (list :id (plist-get m :id) :outcome "ack"))))
          (should (= (length (plist-get s :settled)) 1))))
      (should (equal (rabbitmq-test--error-code 'queue.delete (list :name q)) "confirmation_required"))
      (rabbitmq-test--call 'queue.delete (list :name q :confirm q)))))

(ert-deftest rabbitmq-test-integration-async-consumer-notifications ()
  :tags '(:integration)
  (rabbitmq-test--with-bridge
    (let* ((q (rabbitmq-test--name "c"))
           (buf (rabbitmq-messages-buffer "ert-consumer" '(:kind consumer))))
      (unwind-protect
          (progn
            (rabbitmq-test--call 'queue.declare (list :name q))
            (let ((c (rabbitmq-test--call 'consumer.start (list :queue q :consent t :flush_ms 20))))
              (puthash (rabbitmq-consumer--key "ert" (plist-get c :tag)) buf rabbitmq-consumer--buffers)
              ;; Publish asynchronously; the UI must not block.
              (let ((done nil))
                (rabbitmq-call 'message.publish (list :messages (vector (list :routing_key q :payload '(:data "async"))))
                               :then (lambda (_) (setq done t)))
                (should (rabbitmq-test--wait (lambda () done))))
              (should (rabbitmq-test--wait (lambda () (with-current-buffer buf (= (length rabbitmq-messages--order) 1)))))
              (with-current-buffer buf
                (let ((m (gethash (car rabbitmq-messages--order) rabbitmq-messages--table)))
                  (should (equal (plist-get (plist-get m :payload) :data) "async"))
                  (should (equal (rabbitmq-messages--state m) "unsettled"))
                  ;; Settle through the UI command path.
                  (goto-char (point-min))
                  (rabbitmq-messages-ack)
                  (should (rabbitmq-test--wait (lambda () (equal (rabbitmq-messages--state m) "acked"))))))
              (rabbitmq-test--call 'consumer.cancel (list :consumer (plist-get c :tag)))))
        (kill-buffer buf)
        (ignore-errors (rabbitmq-test--call 'queue.delete (list :name q :confirm q)))))))

(ert-deftest rabbitmq-test-integration-stream-replay ()
  :tags '(:integration)
  (rabbitmq-test--with-bridge
    (let ((s (rabbitmq-test--name "st")))
      (rabbitmq-test--call 'stream.create (list :name s))
      (rabbitmq-test--call 'stream.publish (list :stream s :messages (vector '(:payload (:data "a")) '(:payload (:data "b")))))
      (dotimes (_ 2)
        (let ((r (rabbitmq-test--call 'stream.read (list :stream s :start '(:type "first")))))
          (should (eq (plist-get r :destructive) :json-false))
          (should (equal (mapcar (lambda (m) (plist-get (plist-get m :payload) :data)) (plist-get r :messages)) '("a" "b")))))
      (rabbitmq-test--call 'stream.delete (list :name s :confirm s)))))

(ert-deftest rabbitmq-test-integration-routing-and-topology ()
  :tags '(:integration)
  (rabbitmq-test--with-bridge
    (let ((x (rabbitmq-test--name "x")) (q (rabbitmq-test--name "q")))
      (rabbitmq-test--call 'exchange.declare (list :name x :type "topic"))
      (rabbitmq-test--call 'queue.declare (list :name q))
      (rabbitmq-test--call 'binding.create (list :source x :destination q :routing_key "orders.*"))
      (let ((p (rabbitmq-test--call 'routing.simulate (list :exchange x :routing_key "orders.eu"))))
        (should (eq (plist-get p :prediction) t))
        (should (equal (append (plist-get p :queues) nil) (list q))))
      (should (eq (plist-get (rabbitmq-test--call 'routing.simulate (list :exchange x :routing_key "invoices")) :unroutable) t))
      ;; Topology view renders without error.
      (let ((rabbitmq--sessions rabbitmq--sessions))
        (rabbitmq-topology)
        (should (rabbitmq-test--wait (lambda () (with-current-buffer (format "*rabbitmq-topology %s*" "ert")
                                                  (save-excursion (goto-char (point-min)) (search-forward x nil t))))))
        (kill-buffer (format "*rabbitmq-topology %s*" "ert")))
      (rabbitmq-test--call 'queue.delete (list :name q :confirm q))
      (rabbitmq-test--call 'exchange.delete (list :name x :confirm x)))))

(ert-deftest rabbitmq-test-integration-queues-buffer ()
  :tags '(:integration)
  (rabbitmq-test--with-bridge
    (let ((q (rabbitmq-test--name "list")))
      (rabbitmq-test--call 'queue.declare (list :name q :type "quorum"))
      (rabbitmq-queues)
      (let ((buf (format "*rabbitmq-queues %s*" "ert")))
        (should (rabbitmq-test--wait (lambda () (with-current-buffer buf (assoc q tabulated-list-entries)))))
        (with-current-buffer buf
          (should (equal (rabbitmq-queues--type q) "quorum"))
          (should (string-match-p "sampled" (format "%s" header-line-format))))
        (kill-buffer buf))
      (rabbitmq-test--call 'queue.delete (list :name q :confirm q)))))

(ert-deftest rabbitmq-test-integration-read-only-profile ()
  :tags '(:integration)
  (skip-unless (rabbitmq-test--broker-p))
  (let ((rabbitmq-profiles '(("ro" :host "localhost" :management-url "http://localhost:15672" :password "guest" :read-only t))))
    (unwind-protect
        (progn
          (rabbitmq-rpc-stop)
          (rabbitmq-rpc-call-sync 'session.open (list :session "ro" :profile (rabbitmq-profile-json "ro")) 60)
          (should (equal (condition-case err (progn (rabbitmq-rpc-call-sync 'queue.declare '(:session "ro" :name "rmqel-test.ro")) nil)
                           (rabbitmq-error (nth 2 err)))
                         "read_only_profile"))
          (should (plist-get (rabbitmq-rpc-call-sync 'mgmt.overview '(:session "ro")) :fetched_at)))
      (rabbitmq-rpc-stop))))

(ert-deftest rabbitmq-test-integration-bridge-crash-is-detected ()
  :tags '(:integration)
  (rabbitmq-rpc-stop)
  (rabbitmq-rpc-start)
  (let ((fired nil))
    (add-hook 'rabbitmq-rpc-shutdown-hook (lambda () (setq fired t)))
    (unwind-protect
        (progn
          (kill-process (jsonrpc--process rabbitmq-rpc--connection))
          (should (rabbitmq-test--wait (lambda () fired)))
          (should-not (rabbitmq-rpc-live-p))
          ;; The next call transparently restarts the bridge.
          (should (eq t (plist-get (rabbitmq-rpc-call-sync 'bridge.ping nil) :pong))))
      (remove-hook 'rabbitmq-rpc-shutdown-hook (lambda () (setq fired t)))
      (rabbitmq-rpc-stop))))

(provide 'rabbitmq-test)
;;; rabbitmq-test.el ends here
