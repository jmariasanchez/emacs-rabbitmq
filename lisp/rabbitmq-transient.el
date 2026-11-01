;;; rabbitmq-transient.el --- Transient command menus  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `rabbitmq-dispatch' is the top-level menu.  Commands are grouped by
;; effect so observation and intervention are visually distinct.

;;; Code:

(require 'transient)
(require 'rabbitmq)

(defun rabbitmq-transient--session-desc ()
  (let ((s (ignore-errors (and (rabbitmq-session-names) (rabbitmq-session)))))
    (concat "RabbitMQ  "
            (if s (concat "session: " (propertize s 'face 'bold)
                          (if (rabbitmq-session-read-only-p s) (propertize " [read-only]" 'face 'rabbitmq-read-only) ""))
              (propertize "not connected" 'face 'shadow)))))

;;;###autoload (autoload 'rabbitmq-dispatch "rabbitmq-transient" nil t)
(transient-define-prefix rabbitmq-dispatch ()
  "RabbitMQ commands."
  [:description rabbitmq-transient--session-desc
   ["Sessions"
    ("c" "Connect" rabbitmq-connect)
    ("d" "Disconnect" rabbitmq-disconnect)
    ("C" "Clusters & sessions" rabbitmq-clusters)
    ("!" "Diagnostics (redacted)" rabbitmq-diagnostics)]
   [:description (lambda () (propertize "Observe (no broker changes)" 'face 'rabbitmq-observe))
    ("q" "Queues" rabbitmq-queues)
    ("t" "Topology" rabbitmq-topology)
    ("r" "Predict routing" rabbitmq-route)
    ("s" "Streams" rabbitmq-streams)
    ("S" "Replay a stream" rabbitmq-streams-browse)
    ("T" "Tail a stream" rabbitmq-streams-tail)
    ("m" "Monitor" rabbitmq-monitor)
    ("u" "Consumers" rabbitmq-consumers)
    ("e" "Events" rabbitmq-events)
    ("a" "Audit log" rabbitmq-audit)]]
  [[:description (lambda () (propertize "Consume (changes queue state, asks consent)" 'face 'rabbitmq-consume))
    ("f" "Fetch (basic.get)" rabbitmq-messages-get)
    ("k" "Start consumer" rabbitmq-consumer-start)]
   ["Publish"
    ("p" "Compose message" rabbitmq-producer)
    ("R" "Publish region" rabbitmq-publish-region)
    ("B" "Publish buffer" rabbitmq-publish-buffer)]
   ["Configure"
    ("+q" "Declare queue" rabbitmq-queue-declare)
    ("+x" "Declare exchange" rabbitmq-exchange-declare)
    ("+b" "Create binding" rabbitmq-binding-create)
    ("+s" "Create stream" rabbitmq-stream-create)
    ("A" "Administration" rabbitmq-admin)
    ("E" "Export definitions" rabbitmq-definitions-export)]
   [:description (lambda () (propertize "Destructive (typed confirmation)" 'face 'rabbitmq-destructive))
    ("-q" "Delete queue" rabbitmq-queue-delete)
    ("-p" "Purge queue" rabbitmq-queue-purge)
    ("-x" "Delete exchange" rabbitmq-exchange-delete)
    ("I" "Import definitions" rabbitmq-definitions-import)]])

(provide 'rabbitmq-transient)
;;; rabbitmq-transient.el ends here
