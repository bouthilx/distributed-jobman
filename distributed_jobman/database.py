import logging
import sys

from jobman import sql
from jobman.api0 import open_db
from jobman.tools import flatten, expand

from distributed_jobman import config
from distributed_jobman.utils import Cache, SafeSession


logger = logging.getLogger("database")

cache = Cache(timeout=config["database"]["cache_timeout"])

DB_STRING_TEMPLATE = "postgres://%(username)s:%(password)s@%(database_address)s/%(database_name)s?table=%(table_name)s"


class Database(object):

    def __init__(self, username=None, password=None, address=None, name=None):

        if username is None:
            username = config["database"]["username"]
        if password is None:
            password = config["database"]["password"]
        if address is None:
            address = config["database"]["address"]
        if name is None:
            name = config["database"]["name"]

        self.username = username
        self.password = password
        self.address = address
        self.name = name

        self._dbs = {}
        self._cache_rows = {}

    def _open_db(self, table_name):

        db = self._dbs.get(table_name, None)

        if db is None:
            logger.debug("Open database's table %s..." % table_name)
            db = self._dbs[table_name] = open_db(
                self.get_db_string(table_name))

        return db

    def get_db_string(self, table_name):

        if "-" in table_name:
            raise ValueError("Invalid character for table name: -")
        return (DB_STRING_TEMPLATE %
                dict(username=self.username,
                     password=self.password,
                     database_address=self.address,
                     database_name=self.name,
                     table_name=table_name))

    def update(self, table_name, rows, update_dict):
        db = self._open_db(table_name)

        with SafeSession(db) as safe_session:
            with safe_session.set_timer(60 * 5):
                for row in rows:
                    sql_row = self._load_in_safe_session(
                        db=db, safe_session=safe_session, row_id=row['id'])[0]
                    try:
                        sql_row.update_simple(flatten(update_dict), safe_session.session)
                    except:
                        sys.stderr.write("Failed for row: %d\n" % row['id'])
                safe_session.session.commit()

    def delete(self, table_name, rows):
        db = self._open_db(table_name)

        with SafeSession(db) as safe_session:
            with safe_session.set_timer(60 * 5):
                for row in rows:
                    try:
                        sql_row = self._load_in_safe_session(
                            db=db, safe_session=safe_session, row_id=row['id'])[0]
                        sql_row.delete(safe_session.session)
                    except:
                        sys.stderr.write("Failed for row: %d\n" % row['id'])
                safe_session.session.commit()

    def save(self, table_name, row):
        db = self._open_db(table_name)

        with SafeSession(db) as safe_session:

            if "id" in row:
                row_id = row["id"]
                del row["id"]

                sql_row = self._load_in_safe_session(
                    db=db, safe_session=safe_session, row_id=row_id)[0]
                logger.debug("update row %d" % row_id)
                with safe_session.set_timer(60 * 5):
                    sql_row.update_simple(flatten(row), safe_session.session)
            else:
                logger.debug("insert new row")
                with safe_session.set_timer(60 * 5):
                    sql_row = sql.insert_dict(flatten(row), db,
                                              session=safe_session.session)
                    sql_row._set_in_session(sql.JOBID, sql_row.id,
                                            safe_session.session)

            row_id = sql_row.id

            with safe_session.set_timer(60 * 5):
                safe_session.session.commit()
                logger.debug("session commited")

        # load in new session otherwise lazy attribute selection hangs
        # on forever... why is that!?
        with SafeSession(db) as safe_session:
            sql_row = self._load_in_safe_session(db, safe_session,
                                                 row_id=row_id)[0]
            logger.debug("Fetch all row attributes...")
            eager_dict = self._eager_dicts([sql_row], safe_session)[0]
            logger.debug("Fetch done")

            eager_dict['id'] = row_id

        return eager_dict

    def _eager_dicts(self, lazy_sql_rows, safe_session):
        eager_dicts = [None] * len(lazy_sql_rows)
        with safe_session.set_timer(60 * 5):
            for i, lazy_sql_row in enumerate(lazy_sql_rows):
                eager_dicts[i] = expand(dict(lazy_sql_row.iteritems()))

        return eager_dicts

    def load(self, table_name, filter_eq_dct=None, row_id=None):
        db = self._open_db(table_name)

        with SafeSession(db) as safe_session:

            sql_rows = self._load_in_safe_session(
                db, safe_session, filter_eq_dct, row_id)

            logger.debug("Fetch all row attributes...")
            eager_dicts = self._eager_dicts(sql_rows, safe_session)
            logger.debug("Fetch done")

        for eager_dict in eager_dicts:
            try:
                eager_dict['id'] = eager_dict['jobman']['id']
            except KeyError:
                logger.warning("Row id is broken, deleting and reinserting")
                self.delete(table_name, eager_dict)
                if "id" in eager_dict:
                    logger.debug("%d\n" % eager_dict["id"])
                    del eager_dict["id"]
                eager_dict.update(self.save(table_name, eager_dict))
                logger.debug("%d\n" % eager_dict["id"])

        return eager_dicts

    def _load_in_safe_session(self, db, safe_session,
                              filter_eq_dct=None, row_id=None):

        with safe_session.set_timer(60 * 5):
            logger.debug("Query session...")
            q = db.query(safe_session.session)
            if row_id is not None:
                sql_row = q._query.get(row_id)
                if sql_row is None:
                    raise ValueError("There is no rows with id \"%d\"" % row_id)
                sql_rows = [sql_row]
            elif filter_eq_dct is not None:
                sql_rows = q.filter_eq_dct(filter_eq_dct).all()
            else:
                sql_rows = q.all()
            logger.debug("Query done")

        return sql_rows


database = Database()


def get_db_string(table_name):
    return database.get_db_string(table_name)
