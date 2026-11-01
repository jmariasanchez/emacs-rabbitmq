;;; rabbitmq.el --- RabbitMQ development and administration environment  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 rabbitmq.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;; Author: rabbitmq.el contributors
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1") (transient "0.4"))
;; Keywords: tools, comm

;;; Commentary:

;; A keyboard-driven RabbitMQ environment.  The Rust executable
;; `rabbitmq-bridge' owns every broker connection (AMQP 0-9-1, AMQP 1.0,
;; Streams, Management API); this package is the user interface.
;;
;; Start with `M-x rabbitmq-connect', then `M-x rabbitmq' for the
;; dispatch menu.
;;
;; Safety model (enforced by the bridge, surfaced here):
;;
;; - Observation (listing, inspecting, stream replay, routing prediction)
;;   never changes broker state and never asks for confirmation.
;; - Consuming from an ordinary queue removes or unacks messages; you are
;;   asked for consent every time.
;; - Destructive operations (delete, purge, revoke, import) require you to
;;   type the target's name.
;; - Profiles marked :read-only reject every intervention.
;; - Every intervention is recorded in the audit log (`rabbitmq-audit').

;;; Code:

(require 'rabbitmq-core)

;;;; Entry point

(declare-function rabbitmq-dispatch "rabbitmq-transient")

;;;###autoload
(defun rabbitmq ()
  "Open the RabbitMQ command menu."
  (interactive)
  (require 'rabbitmq-transient)
  (call-interactively #'rabbitmq-dispatch))

(require 'rabbitmq-events)
(require 'rabbitmq-clusters)
(require 'rabbitmq-queues)
(require 'rabbitmq-topology)
(require 'rabbitmq-messages)
(require 'rabbitmq-producer)
(require 'rabbitmq-consumer)
(require 'rabbitmq-streams)
(require 'rabbitmq-admin)
(require 'rabbitmq-monitor)

(provide 'rabbitmq)
;;; rabbitmq.el ends here
