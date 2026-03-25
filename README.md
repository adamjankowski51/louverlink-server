# LouverLink Mosquitto ACL
# ─────────────────────────────────────────────────────────────────────────────
#
# Deploy this file to /etc/mosquitto/acl on the server.
# After deploying: chown mosquitto /etc/mosquitto/acl
#
# IMPORTANT: Do NOT use %c (client ID substitution) for the credentials topic.
# Mosquitto silently blocks subscriptions when %c is used — use + wildcard instead.
#
# Registration user — strictly limited to publishing registration requests.
# Can read credentials topic so device receives its unique credentials after registration.
user louverlink_reg
topic write louverlink/register
topic read louverlink/+/credentials

# Server service account — full access to manage all device topics.
user louverlink
topic readwrite louverlink/#

# Per-device ACL entries are appended below by the Node server
# when device credentials are generated at registration time.
# Format:
#   user device_ll_XXXXXXXXXXXX
#   topic readwrite louverlink/LL-XXXXXXXXXXXX/#
#
# ─── Per-device entries (auto-managed, do not edit manually) ─────────────────
