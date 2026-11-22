#!/usr/bin/env bash
# Creates a cluster CA and one PKCS12 keystore per node, each signed by that CA.
# Every node keystore holds the node's own key plus the CA certificate, which is
# the shape TlsContexts.fromKeystore expects. Copy <node>.p12 to that node only.
#
# Usage: scripts/gen-cluster-certs.sh <out-dir> <password> <node> [<node> ...]
# Example: scripts/gen-cluster-certs.sh certs changeit pi mac
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: $0 <out-dir> <password> <node> [<node> ...]" >&2
  exit 1
fi

OUT=$1
PASS=$2
shift 2
mkdir -p "$OUT"

CA="$OUT/ca.p12"
rm -f "$CA" "$OUT/ca.pem"
keytool -genkeypair -alias ca -keyalg EC -groupname secp256r1 \
  -dname "CN=EdgeActors Cluster CA" -ext bc:c -validity 825 \
  -storetype PKCS12 -keystore "$CA" -storepass "$PASS" -keypass "$PASS"
keytool -exportcert -rfc -alias ca -keystore "$CA" -storepass "$PASS" -file "$OUT/ca.pem"

for NODE in "$@"; do
  KS="$OUT/$NODE.p12"
  rm -f "$KS"
  keytool -genkeypair -alias node -keyalg EC -groupname secp256r1 \
    -dname "CN=$NODE" -validity 825 \
    -storetype PKCS12 -keystore "$KS" -storepass "$PASS" -keypass "$PASS"
  keytool -certreq -alias node -keystore "$KS" -storepass "$PASS" -file "$OUT/$NODE.csr"
  keytool -gencert -alias ca -keystore "$CA" -storepass "$PASS" \
    -infile "$OUT/$NODE.csr" -outfile "$OUT/$NODE.pem" -rfc -validity 825 \
    -ext ku:c=digitalSignature -ext eku=serverAuth,clientAuth
  keytool -importcert -noprompt -alias ca -file "$OUT/ca.pem" -keystore "$KS" -storepass "$PASS"
  keytool -importcert -alias node -file "$OUT/$NODE.pem" -keystore "$KS" -storepass "$PASS"
  rm -f "$OUT/$NODE.csr" "$OUT/$NODE.pem"
  echo "created $KS"
done
