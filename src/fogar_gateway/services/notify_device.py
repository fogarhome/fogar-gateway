"""
notify_device.py
~~~~~~~~~~~~~~~~
El dispositivo de avisos (5.0, #407): dónde suena un aviso. Uno del hogar (`homes`) y uno por
persona (`memberships`), FK a `clients` con ON DELETE SET NULL.

Reglas (las comprueba el gateway, no solo la web):
- El del hogar: cualquier aparato activo de ese hogar, del hogar o personal (un aviso común puede
  sonar en el móvil de alguien).
- El de una persona: solo uno de sus aparatos personales activos. Un aviso privado nunca llega a
  un aparato del hogar ni a uno de otra persona.
- «Activo»: no retirado y `is_active`; si es personal, de una persona activa.

Retirar un aparato (o borrarlo con su persona) lo quita de donde estuviera elegido
(`clear_notify_device`). Desactivarlo (`is_active` a falso, o desactivar a su persona) no lo
quita: la elección vuelve a valer al reactivarlo, y quien reparta los avisos ignora un aparato
inactivo.
"""

from sqlmodel import Session, col, update

from fogar_gateway.db.models import ClientRecord, HomeRecord, MembershipRecord, UserRecord

# El `detail` del 422 cuando el aparato elegido no vale (desconocido, retirado, inactivo, de otro
# hogar o, para una persona, que no es suyo).
INVALID_NOTIFY_DEVICE = "invalid_notify_device"


def _active(session: Session, rec: ClientRecord | None, home_id: str) -> ClientRecord | None:
    if rec is None or rec.home_id != home_id or rec.retired_at is not None or not rec.is_active:
        return None
    if rec.user_id is not None:
        user = session.get(UserRecord, rec.user_id)
        if user is None or not user.is_active:
            return None
    return rec


def home_device_ok(session: Session, home_id: str, client_id: str) -> bool:
    """`client_id` puede ser el dispositivo de avisos del hogar `home_id`."""
    return _active(session, session.get(ClientRecord, client_id), home_id) is not None


def personal_device_ok(session: Session, user_id: str, home_id: str, client_id: str) -> bool:
    """`client_id` puede ser el dispositivo de avisos de `user_id` en `home_id`: uno suyo."""
    rec = _active(session, session.get(ClientRecord, client_id), home_id)
    return rec is not None and rec.user_id == user_id


def clear_notify_device(session: Session, client_id: str) -> None:
    """Quita `client_id` de donde esté elegido (el hogar y las personas). Sin `commit`: va en la
    misma transacción que lo retira o lo borra."""
    for table in (HomeRecord, MembershipRecord):
        session.exec(  # type: ignore[call-overload]
            update(table)
            .where(col(table.notify_client_id) == client_id)
            .values(notify_client_id=None)
        )
