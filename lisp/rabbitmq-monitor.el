;;; rabbitmq-monitor.el --- Live cluster dashboard and diagnostics  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `rabbitmq-monitor-mode' is a dashboard refreshed on a timer: cluster
;; totals and rates, per-node memory/disk/fd/socket usage with alarms,
;; health checks, the deepest queues, and dead-letter configuration.
;;
;; Every section shows when its data was sampled.  Management API
;; statistics are periodic samples, not real-time counters; values older
;; than `rabbitmq-monitor-stale-after' seconds are shown dimmed, and
;; sections whose request failed say so instead of showing old numbers.

;;; Code:

(require 'rabbitmq-core)

(defcustom rabbitmq-monitor-interval 5
  "Seconds between dashboard refreshes."
  :type 'number
  :group 'rabbitmq)

(defcustom rabbitmq-monitor-stale-after 15
  "Seconds after which a dashboard section is considered stale."
  :type 'number
  :group 'rabbitmq)

(defcustom rabbitmq-monitor-top-queues 10
  "Number of deepest queues shown."
  :type 'integer
  :group 'rabbitmq)

(defvar-local rabbitmq-monitor--timer nil)
(defvar-local rabbitmq-monitor--sections nil "Alist section -> (:at MS :data D :error E).")
(defvar-local rabbitmq-monitor--history nil "Ring of (at . messages) for a sparkline.")

(defvar-keymap rabbitmq-monitor-mode-map
  "g" #'rabbitmq-monitor-refresh
  "p" #'rabbitmq-monitor-toggle-pause
  "D" #'rabbitmq-diagnostics
  "e" #'rabbitmq-monitor-export)

(define-derived-mode rabbitmq-monitor-mode special-mode "RMQ-Monitor"
  "Live RabbitMQ dashboard.  g refresh, p pause, e export report, D diagnostics.
\\{rabbitmq-monitor-mode-map}"
  (setq-local truncate-lines t)
  (add-hook 'kill-buffer-hook #'rabbitmq-monitor--stop nil t))

(defun rabbitmq-monitor--stop ()
  (when rabbitmq-monitor--timer (cancel-timer rabbitmq-monitor--timer) (setq rabbitmq-monitor--timer nil)))

(defun rabbitmq-monitor--fetch (section method params)
  (let ((buf (current-buffer)))
    (rabbitmq-call method params
                   :then (lambda (r)
                           (when (buffer-live-p buf)
                             (with-current-buffer buf
                               (setf (alist-get section rabbitmq-monitor--sections)
                                     (list :at (or (plist-get r :fetched_at) (truncate (* 1000 (float-time))))
                                           :data (if (plist-member r :data) (plist-get r :data) r)))
                               (rabbitmq-monitor--render))))
                   :else (lambda (e)
                           (when (buffer-live-p buf)
                             (with-current-buffer buf
                               (let ((old (alist-get section rabbitmq-monitor--sections)))
                                 (setf (alist-get section rabbitmq-monitor--sections)
                                       (plist-put (copy-sequence old) :error (plist-get e :message))))
                               (rabbitmq-monitor--render)))))))

(defun rabbitmq-monitor-refresh ()
  "Refresh every dashboard section."
  (interactive)
  (let ((vh (rabbitmq-session-vhost)))
    (rabbitmq-monitor--fetch 'overview 'mgmt.overview nil)
    (rabbitmq-monitor--fetch 'nodes 'mgmt.nodes nil)
    (rabbitmq-monitor--fetch 'health 'mgmt.health nil)
    (rabbitmq-monitor--fetch 'queues 'mgmt.queues (list :vhost vh :page 1 :page_size rabbitmq-monitor-top-queues
                                                       :sort "messages" :sort_reverse t))
    (rabbitmq-monitor--fetch 'dlx 'mgmt.dead_letter_report (list :vhost vh))))

(defun rabbitmq-monitor-toggle-pause ()
  "Pause or resume automatic refresh."
  (interactive)
  (if rabbitmq-monitor--timer
      (progn (rabbitmq-monitor--stop) (message "rabbitmq: monitor paused"))
    (rabbitmq-monitor--start)
    (message "rabbitmq: monitor resumed")))

(defun rabbitmq-monitor--start ()
  (let ((buf (current-buffer)))
    (setq rabbitmq-monitor--timer
          (run-with-timer 0 rabbitmq-monitor-interval
                          (lambda ()
                            (if (buffer-live-p buf)
                                (when (get-buffer-window buf 'visible)
                                  (with-current-buffer buf (rabbitmq-monitor-refresh)))
                              (with-current-buffer buf (rabbitmq-monitor--stop))))))))

;;;###autoload
(defun rabbitmq-monitor ()
  "Open the live dashboard for the current session."
  (interactive)
  (rabbitmq-with-buffer (format "*rabbitmq-monitor %s*" (rabbitmq-session)) 'rabbitmq-monitor-mode
    (unless rabbitmq-monitor--timer (rabbitmq-monitor--start))))

(defun rabbitmq-monitor--section-header (title section)
  (let* ((s (alist-get section rabbitmq-monitor--sections))
         (at (plist-get s :at))
         (age (and at (/ (- (* 1000 (float-time)) at) 1000.0)))
         (stale (or (null age) (> age rabbitmq-monitor-stale-after))))
    (concat (propertize title 'face 'bold)
            (cond ((plist-get s :error) (propertize (format "  UNAVAILABLE: %s" (plist-get s :error)) 'face 'rabbitmq-destructive))
                  ((null at) (propertize "  loading..." 'face 'rabbitmq-stale))
                  (t (propertize (format "  sampled %s%s" (rabbitmq--age at) (if stale " (STALE)" "")) 'face (if stale 'rabbitmq-stale 'shadow))))
            "\n")))

(defun rabbitmq-monitor--rate (stats key)
  (let ((d (plist-get stats (intern (format ":%s_details" key)))))
    (if d (format "%.1f/s" (or (plist-get d :rate) 0)) (propertize "n/a" 'face 'rabbitmq-stale))))

(defun rabbitmq-monitor--bar (used limit width)
  (if (and (numberp used) (numberp limit) (> limit 0))
      (let* ((frac (min 1.0 (/ (float used) limit)))
             (n (round (* frac width))))
        (propertize (concat (make-string n ?#) (make-string (- width n) ?.))
                    'face (cond ((> frac 0.9) 'rabbitmq-destructive) ((> frac 0.7) 'rabbitmq-consume) (t 'rabbitmq-observe))))
    (propertize (make-string width ??) 'face 'rabbitmq-stale)))

(defun rabbitmq-monitor--sparkline (values)
  (let ((chars " ▁▂▃▄▅▆▇█")
        (mx (apply #'max 1 values)))
    (mapconcat (lambda (v) (string (aref chars (min 8 (round (* 8 (/ (float v) mx))))))) values "")))

(defun rabbitmq-monitor--render ()
  (let* ((inhibit-read-only t)
         (pos (point))
         (ov (plist-get (alist-get 'overview rabbitmq-monitor--sections) :data))
         (nodes (rabbitmq--vec (plist-get (alist-get 'nodes rabbitmq-monitor--sections) :data)))
         (health (plist-get (alist-get 'health rabbitmq-monitor--sections) :data))
         (queues (rabbitmq--items (plist-get (alist-get 'queues rabbitmq-monitor--sections) :data)))
         (dlx (rabbitmq--vec (plist-get (plist-get (alist-get 'dlx rabbitmq-monitor--sections) :data) :queues))))
    (when-let* ((m (plist-get (plist-get ov :queue_totals) :messages)))
      (push m rabbitmq-monitor--history)
      (setq rabbitmq-monitor--history (seq-take rabbitmq-monitor--history 60)))
    (erase-buffer)
    (setq header-line-format (concat (rabbitmq--header "Monitor") (if rabbitmq-monitor--timer (format "  every %ss" rabbitmq-monitor-interval) "  PAUSED")))
    ;; Overview
    (insert (rabbitmq-monitor--section-header "Cluster" 'overview))
    (when ov
      (let ((tot (plist-get ov :object_totals)) (qt (plist-get ov :queue_totals)) (ms (plist-get ov :message_stats)))
        (insert (format "  %s %s  cluster=%s  erlang %s\n" (or (plist-get ov :product_name) "RabbitMQ") (plist-get ov :rabbitmq_version)
                        (plist-get ov :cluster_name) (plist-get ov :erlang_version)))
        (insert (format "  connections %s  channels %s  queues %s  consumers %s  exchanges %s\n"
                        (rabbitmq--num (plist-get tot :connections)) (rabbitmq--num (plist-get tot :channels))
                        (rabbitmq--num (plist-get tot :queues)) (rabbitmq--num (plist-get tot :consumers))
                        (rabbitmq--num (plist-get tot :exchanges))))
        (insert (format "  messages %s (ready %s, unacked %s)  %s\n"
                        (rabbitmq--num (plist-get qt :messages)) (rabbitmq--num (plist-get qt :messages_ready))
                        (rabbitmq--num (plist-get qt :messages_unacknowledged))
                        (rabbitmq-monitor--sparkline (reverse rabbitmq-monitor--history))))
        (insert (format "  publish %s  deliver %s  ack %s  redeliver %s  unroutable(drop) %s  unroutable(return) %s\n\n"
                        (rabbitmq-monitor--rate ms "publish") (rabbitmq-monitor--rate ms "deliver_get")
                        (rabbitmq-monitor--rate ms "ack") (rabbitmq-monitor--rate ms "redeliver")
                        (rabbitmq-monitor--rate ms "drop_unroutable") (rabbitmq-monitor--rate ms "return_unroutable")))))
    ;; Nodes
    (insert (rabbitmq-monitor--section-header "Nodes" 'nodes))
    (dolist (n nodes)
      (insert (format "  %-32s %s%s%s\n" (plist-get n :name)
                      (if (eq (plist-get n :running) t) (propertize "running" 'face 'rabbitmq-observe) (propertize "DOWN" 'face 'rabbitmq-destructive))
                      (if (eq (plist-get n :mem_alarm) t) (propertize "  MEMORY ALARM" 'face 'rabbitmq-destructive) "")
                      (if (eq (plist-get n :disk_free_alarm) t) (propertize "  DISK ALARM" 'face 'rabbitmq-destructive) "")))
      (insert (format "    mem  %s %s / %s\n" (rabbitmq-monitor--bar (plist-get n :mem_used) (plist-get n :mem_limit) 30)
                      (rabbitmq--bytes (plist-get n :mem_used)) (rabbitmq--bytes (plist-get n :mem_limit))))
      (insert (format "    disk free %s (limit %s)\n" (rabbitmq--bytes (plist-get n :disk_free)) (rabbitmq--bytes (plist-get n :disk_free_limit))))
      (insert (format "    fds  %s %s / %s   sockets %s / %s   procs %s / %s   uptime %s\n"
                      (rabbitmq-monitor--bar (plist-get n :fd_used) (plist-get n :fd_total) 20)
                      (rabbitmq--num (plist-get n :fd_used)) (rabbitmq--num (plist-get n :fd_total))
                      (rabbitmq--num (plist-get n :sockets_used)) (rabbitmq--num (plist-get n :sockets_total))
                      (rabbitmq--num (plist-get n :proc_used)) (rabbitmq--num (plist-get n :proc_total))
                      (if (numberp (plist-get n :uptime)) (format-seconds "%dd %hh %mm" (/ (plist-get n :uptime) 1000)) "?")))
      (when-let* ((parts (rabbitmq--vec (plist-get n :partitions))))
        (insert (propertize (format "    NETWORK PARTITION with %s\n" (string-join parts ", ")) 'face 'rabbitmq-destructive))))
    (insert "\n")
    ;; Health
    (insert (rabbitmq-monitor--section-header "Health checks" 'health))
    (cl-loop for (k v) on health by #'cddr
             do (insert (format "  %-26s %s\n" (substring (symbol-name k) 1)
                                (if (eq (plist-get v :ok) t) (propertize "ok" 'face 'rabbitmq-observe)
                                  (propertize (format "FAILED %s" (or (plist-get (plist-get v :body) :reason) (plist-get v :error) (plist-get v :http_status) ""))
                                              'face 'rabbitmq-destructive)))))
    (insert "\n")
    ;; Queues
    (insert (rabbitmq-monitor--section-header (format "Deepest queues in %s" (rabbitmq-session-vhost)) 'queues))
    (dolist (q queues)
      (let ((ms (plist-get q :message_stats)))
        (insert (format "  %-40s %-8s %8s ready %8s unacked %4s consumers  in %s  out %s\n"
                        (truncate-string-to-width (plist-get q :name) 40) (or (plist-get q :type) "")
                        (rabbitmq--num (plist-get q :messages_ready)) (rabbitmq--num (plist-get q :messages_unacknowledged))
                        (rabbitmq--num (plist-get q :consumers))
                        (rabbitmq-monitor--rate ms "publish") (rabbitmq-monitor--rate ms "deliver_get")))))
    (insert "\n")
    ;; Dead-lettering
    (insert (rabbitmq-monitor--section-header "Dead-letter configuration (targets are predictions)" 'dlx))
    (if (null dlx) (insert "  none configured\n")
      (dolist (d dlx)
        (insert (format "  %-36s -> %-24s %s  strategy=%s%s\n" (plist-get d :queue)
                        (let ((x (plist-get d :dead_letter_exchange))) (if (string-empty-p x) "(default)" x))
                        (cond ((not (eq (plist-get d :exchange_exists) t)) (propertize "EXCHANGE MISSING: dead letters are dropped" 'face 'rabbitmq-destructive))
                              ((eq (plist-get d :dead_letters_dropped) t) (propertize "no route: dead letters are dropped" 'face 'rabbitmq-consume))
                              (t (string-join (rabbitmq--vec (plist-get d :predicted_targets)) ",")))
                        (plist-get d :strategy)
                        (if-let* ((l (plist-get d :delivery_limit))) (format "  delivery-limit=%s" l) "")))))
    (goto-char (min pos (point-max)))))

(defun rabbitmq-monitor-export (file)
  "Write the current dashboard data plus a redacted diagnostic report to FILE."
  (interactive "FExport diagnostic report to: ")
  (let ((sections rabbitmq-monitor--sections))
    (rabbitmq-rpc-call 'bridge.diagnostics nil
                       :then (lambda (diag)
                               (with-temp-file file
                                 (insert (json-serialize (list :generated_at (format-time-string "%FT%T%z")
                                                               :dashboard (cl-loop for (k . v) in sections
                                                                                   append (list (intern (format ":%s" k)) (or (plist-get v :data) (make-hash-table))))
                                                               :diagnostics diag)
                                                         :null-object nil :false-object :json-false))
                                 (json-pretty-print-buffer))
                               (message "rabbitmq: wrote %s (credentials redacted by the bridge)" file)))))

(declare-function rabbitmq-diagnostics "rabbitmq-clusters")

(provide 'rabbitmq-monitor)
;;; rabbitmq-monitor.el ends here
