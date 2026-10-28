"""Démonstration (formateur) : pourquoi un hash simple N'ANONYMISE PAS un téléphone.

Un numéro français mobile = +336 + 10 chiffres ou ivoirien mobile = +225 + 10 chiffres dont les préfixes sont connus (01, 05, 07).
L'espace réel est d'environ 3 x 10^8 valeurs : un PC portable le parcourt en quelques minutes.
On montre ici l'attaque sur un sous-espace (préfixe connu) pour qu'elle tienne en quelques secondes.

    python jour1/TP2_qualite_rgpd/corrige/demo_hash_dictionnaire.py
"""
import hashlib
import hmac
import secrets
import time

victime = "+2250701234567"
fuite_sha256 = hashlib.sha256(victime.encode()).hexdigest()
print(f"Hash SHA-256 'anonymisé' publié : {fuite_sha256[:32]}...")

# L'attaquant connaît le format et le préfixe opérateur, il énumère les 10^6 suffixes
t0 = time.perf_counter()
for i in range(1_000_000):
    candidat = f"+2250701{i:06d}"
    if hashlib.sha256(candidat.encode()).hexdigest() == fuite_sha256:
        print(f"→ Ré-identifié en {time.perf_counter() - t0:.2f}s : {candidat}")
        break

cle = secrets.token_bytes(32)
pseudo = hmac.new(cle, victime.encode(), hashlib.sha256).hexdigest()
print(f"\nAvec HMAC + clé secrète : {pseudo[:32]}...")
print("Sans la clé (stockée dans un coffre, hors du jeu de données), l'énumération est inutile.")
print("Mais attention : avec la clé, la ré-identification reste possible -> c'est une PSEUDONYMISATION.")
