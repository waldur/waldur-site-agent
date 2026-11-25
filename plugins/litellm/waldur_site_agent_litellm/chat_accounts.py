"""Which chat accounts are the plugin's, and the password it gives the ones it creates.

The plugin finds a person's Open WebUI account by email, and an email proves nothing on
its own: with ``ENABLE_SIGNUP`` on, anyone can register an address before its owner is
provisioned, and an operator may have made an account by hand. Acting on whatever the
lookup returns promoted a squatter's ``pending`` account onto someone else's entitlement,
and deleted accounts the plugin never made. So the plugin records the id of every account
it creates or *binds* on the person's LiteLLM user, and only grants access to, or deletes,
that account.

An account is bound -- adopted as the plugin's -- when its origin vouches for the address.
Under ``sso`` every account does: the identity provider makes them all, with signup off, and
those from LDAP or trusted-header sign-in carry no mark of it to check. Under
``managed_password`` it must be one of:

- it is **IdP-backed**: Open WebUI made it at an OIDC login (``oauth``, ``oauth_sub`` on
  builds before the ``oauth`` column) or through SCIM, so the identity provider asserted
  the address; or
- it was **created before the migration cutoff** (``bind_accounts_created_before``) for a
  person provisioned before it too: an account that predates this module, which the plugin
  made under ``managed_password`` but had nowhere to record.

Taking access *away* needs no binding: demoting an account to ``pending`` on removal ends
access the person is no longer entitled to, whoever made it, and it is reversible. It is
granting and deleting that are held to provenance -- which, under ``sso``, every account
under the address has.

The derived password
--------------------
Waldur has no channel to hand one person a secret, so ``managed_password`` used to give
every account the operator's one ``initial_password``, and any member could sign in as any
other who had not changed it. Each account now gets ``HMAC-SHA256(password_secret,
address)``, so a person told their own password cannot compute anyone else's. The key is a
new setting rather than ``initial_password``: every member was told that one, and the
derivation is public, so anyone holding it could compute every password. The operator
prints a person's password with ``waldur-litellm-chat-password`` and hands it out.
"""

from __future__ import annotations

import argparse
import base64
import datetime
import getpass
import hashlib
import hmac
import os
import sys
import unicodedata

# The environment variable the password CLI reads the secret from, when set.
SECRET_ENV = "WALDUR_OPENWEBUI_PASSWORD_SECRET"  # noqa: S105 - a variable name

# Characters of the HMAC's base64 kept in the password: 24 x 6 = 144 bits.
_PASSWORD_HMAC_CHARS = 24
# Appended so the password meets Open WebUI's ``PASSWORD_VALIDATION_REGEX`` default --
# an upper- and a lower-case letter, a digit and a symbol -- whatever the HMAC came out as.
# It adds no secrecy, and needs none.
_PASSWORD_SUFFIX = ".Wa1"  # noqa: S105 - a fixed suffix, not a secret


def canonical_email(address: str) -> str:
    """The form an address is compared and keyed in: NFKC, then casefolded.

    ``lower()`` alone leaves compatibility forms distinct -- a fullwidth ``U+FF41`` is not
    ``a`` -- and so two strings a person reads as the same address could be two accounts.
    NFKC folds those first; casefold then also covers what ``lower()`` misses (``ß``).
    Plain ASCII comes out exactly as ``strip().lower()`` did, so no existing record moves.
    """
    return unicodedata.normalize("NFKC", str(address)).casefold().strip()


def derived_password(secret: str, address: str) -> str:
    """The password the plugin gives the account it creates for ``address``."""
    digest = hmac.new(
        str(secret).encode(), canonical_email(address).encode(), hashlib.sha256
    ).digest()
    token = base64.urlsafe_b64encode(digest).decode().rstrip("=")
    return f"{token[:_PASSWORD_HMAC_CHARS]}{_PASSWORD_SUFFIX}"


def parse_cutoff(value: object) -> int | None:
    """Read ``bind_accounts_created_before`` as epoch seconds, or None when unset.

    A date means midnight UTC at its start, so accounts made on that day are not bound.
    A datetime without a zone is read as UTC too. Raises ``ValueError`` on anything else.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime.datetime):
        moment = value
    elif isinstance(value, datetime.date):
        moment = datetime.datetime(value.year, value.month, value.day)
    else:
        text = str(value).strip()
        # Python before 3.11 rejects the "Z" suffix that ISO 8601 allows for UTC.
        if text.endswith(("Z", "z")):
            text = f"{text[:-1]}+00:00"
        moment = datetime.datetime.fromisoformat(text)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=datetime.timezone.utc)
    return int(moment.timestamp())


def is_identity_backed(account: dict) -> bool:
    """Whether Open WebUI made this account from an identity provider's assertion."""
    return bool(account.get("oauth") or account.get("oauth_sub") or account.get("scim"))


def created_before(account: dict, cutoff: int | None) -> bool:
    """Whether the account predates the migration cutoff. False with no cutoff set."""
    if cutoff is None:
        return False
    try:
        created = int(account.get("created_at"))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    return created < cutoff


def main(argv: list | None = None) -> int:
    """Print the chat password the plugin gives each address on the command line."""
    parser = argparse.ArgumentParser(
        prog="waldur-litellm-chat-password",
        description=(
            "Print the Open WebUI password the LiteLLM plugin gives an account it creates "
            "under account_provisioning: managed_password. The secret is the offering's "
            f"openwebui.password_secret, read from ${SECRET_ENV} or prompted for."
        ),
    )
    parser.add_argument("emails", nargs="+", metavar="EMAIL")
    args = parser.parse_args(argv)
    secret = os.environ.get(SECRET_ENV) or getpass.getpass("password_secret: ")
    if not secret:
        parser.error("no password_secret given")
    for email in args.emails:
        sys.stdout.write(f"{canonical_email(email)}\t{derived_password(secret, email)}\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
