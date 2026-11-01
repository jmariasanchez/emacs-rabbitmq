#!/usr/bin/env bash
# Build a package.el-installable tarball of the Emacs Lisp package.
#
# Usage: scripts/package-lisp.sh OUTDIR
# Produces OUTDIR/rabbitmq-VERSION.tar (install with M-x package-install-file)
# and verifies it installs and loads in a clean Emacs.
set -euo pipefail

out=${1:?usage: package-lisp.sh OUTDIR}
root=$(cd "$(dirname "$0")/.." && pwd)
emacs=${EMACS:-emacs}

version=$(sed -n 's/^;; Version: *//p' "$root/lisp/rabbitmq.el")
requires=$(sed -n 's/^;; Package-Requires: *//p' "$root/lisp/rabbitmq.el")
url=$(sed -n 's/^;; URL: *//p' "$root/lisp/rabbitmq.el")
[ -n "$version" ] && [ -n "$requires" ] || { echo "missing Version/Package-Requires header" >&2; exit 1; }

mkdir -p "$out"
out=$(cd "$out" && pwd)
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
pkg="$stage/rabbitmq-$version"
mkdir "$pkg"
cp "$root"/lisp/*.el "$root/README.md" "$root/COPYING" "$pkg/"
cat > "$pkg/rabbitmq-pkg.el" <<EOF
;; -*- no-byte-compile: t; lexical-binding: nil -*-
(define-package "rabbitmq" "$version"
  "RabbitMQ development and administration environment"
  '$requires
  :url "$url"
  :keywords '("tools" "comm"))
EOF

tarball="$out/rabbitmq-$version.tar"
COPYFILE_DISABLE=1 tar --format=ustar -C "$stage" -cf "$tarball" "rabbitmq-$version"

# Install into a throwaway package directory and load it. Refresh archive
# contents first, as a user would: Emacs 29.1's package.el does not count the
# bundled Transient as installed, so it fetches Transient from GNU ELPA.
elpa="$stage/elpa"
"$emacs" --batch -Q --eval "(progn
  (setq package-user-dir \"$elpa\")
  (package-initialize)
  (package-refresh-contents)
  (package-install-file \"$tarball\")
  (require 'rabbitmq)
  (unless (and (featurep 'rabbitmq) (commandp 'rabbitmq-connect)
               (string-prefix-p \"$elpa\" (locate-library \"rabbitmq\")))
    (error \"rabbitmq did not load from the installed package\"))
  (message \"package-install OK: %s\" (locate-library \"rabbitmq\")))"
echo "$tarball"
