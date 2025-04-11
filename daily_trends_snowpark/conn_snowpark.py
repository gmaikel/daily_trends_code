# session_manager.py
from snowflake.snowpark import Session

class SessionManager:
    _session = None

    @staticmethod
    def get_session():
        if SessionManager._session is None:
            SessionManager._session = Session.builder.config("connection_name", "myconnection").create()
        return SessionManager._session

