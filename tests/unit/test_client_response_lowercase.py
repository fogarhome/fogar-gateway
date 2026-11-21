"""Revisión de C-1: una fila de la 2.x con agentes en mayúsculas se devuelve en minúsculas."""

import json

from fogar_gateway.db.models import ClientRecord
from fogar_gateway.models.admin_schemas import ClientResponse
from fogar_gateway.services.auth import hash_key


def test_old_row_agents_are_returned_lowercase():
    r = ClientRecord(id="c1", name="tele", key_hash=hash_key("k" * 43), default_agent="Main",
                     allowed_agents=json.dumps(["Main", "otro"]), is_active=True,
                     barge_in_enabled=True, barge_in_min_chars=3, push_enabled=True,
                     tool_calls_enabled=False)
    resp = ClientResponse.from_record(r)
    assert resp.default_agent == "main" and resp.allowed_agents == ["main", "otro"]
