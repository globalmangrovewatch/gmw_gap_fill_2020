from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import Engine
from sqlalchemy import event
import sqlalchemy
from sqlalchemy.orm.attributes import flag_modified
from sqlite3 import Connection as SQLite3Connection
import logging
import os
import shutil

logger = logging.getLogger(__name__)

Base = declarative_base()

class Sen2Process(Base):
    __tablename__ = "Sen2Process"
    product_id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
    scn_url = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    granule = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    download = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    download_path = sqlalchemy.Column(sqlalchemy.String, nullable=True)
    ard = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False, default=False)
    ard_path = sqlalchemy.Column(sqlalchemy.String, nullable=True)


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode = MEMORY")
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.execute("PRAGMA temp_store = MEMORY")
        cursor.execute("PRAGMA cache_size = 500000")
        cursor.close()


class RecordSen2Process(object):
    
    def __init__(self, sqlite_db_file):
        self.sqlite_db_file = sqlite_db_file
        self.sqlite_db_conn = "sqlite:///{}".format(self.sqlite_db_file)
    
    def init_db(self):
        """
        """
        try:
            logger.debug("Creating Database Engine and Session.")
            db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
            Base.metadata.drop_all(db_engine)
            logger.debug("Creating Database.")
            Base.metadata.bind = db_engine
            Base.metadata.create_all()
            session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
            ses = session_sqlalc()
            ses.close()
            logger.debug("Created Database Engine and Session.")
        except:
            raise Exception("The SQLite database file cannot be opened: '{}'".format(sqlite_db_conn))
    
    def add_sen2_scns(self, sen2_scns_lst):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        scn_lst = []
        for scn in sen2_scns_lst:
            scn_lst.append(Sen2Process(product_id=scn['product_id'], scn_url=scn['scn_url'], granule=scn['granule']))

        logger.debug("There are {} scenes to be written to the database.".format(len(scn_lst)))
        if len(scn_lst) > 0:
            ses.add_all(scn_lst)
            ses.commit()
            logger.debug("Written jobs to the database.")
        ses.close()
    
    def is_scn_in_db(self, product_id):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")
        
        logger.debug("Perform query to find scene.")
        query_result = ses.query(Sen2Process).filter(Sen2Process.product_id == product_id).one_or_none()
        ses.close()
        logger.debug("Closed the database session.")
        found_prod_id = True
        if query_result is None:
            found_prod_id = False        
        return found_prod_id
    
    def n_granule_scns(self, granule):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")
        
        logger.debug("Perform query to find scene.")
        n_scns = ses.query(Sen2Process).filter(Sen2Process.granule == granule).count()
        ses.close()
        logger.debug("Closed the database session.")     
        return n_scns

    def granule_scns(self, granule):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        logger.debug("Perform query to find scene.")
        query_result = ses.query(Sen2Process).filter(Sen2Process.granule == granule).all()
        scns = list()
        for scn in query_result:
            scns.append(scn)
        ses.close()
        logger.debug("Closed the database session.")
        return scns

    def set_scn_downloaded(self, product_id, download_path):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        logger.debug("Perform query to find scene.")
        query_result = ses.query(Sen2Process).filter(Sen2Process.product_id == product_id).one_or_none()
        if query_result is not None:
            query_result.download = True
            query_result.download_path = download_path
            ses.commit()
        ses.close()
        logger.debug("Closed the database session.")

    def get_scns_download(self, granule=None):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        logger.debug("Perform query to find scene.")
        if granule is None:
            query_result = ses.query(Sen2Process).filter(Sen2Process.download == False).all()
        else:
            query_result = ses.query(Sen2Process).filter(Sen2Process.granule == granule,
                                                         Sen2Process.download == False).all()
        scns = list()
        for scn in query_result:
            scns.append(scn)
        ses.close()
        logger.debug("Closed the database session.")
        return scns

    def is_scn_downloaded(self, product_id):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        logger.debug("Perform query to find scene.")
        query_result = ses.query(Sen2Process).filter(Sen2Process.product_id == product_id).one_or_none()
        ses.close()
        logger.debug("Closed the database session.")
        downloaded = False
        if query_result is not None:
            downloaded = query_result.download
        return downloaded

    def set_scn_ard(self, product_id, ard_path):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        logger.debug("Perform query to find scene.")
        query_result = ses.query(Sen2Process).filter(Sen2Process.product_id == product_id).one_or_none()
        if query_result is not None:
            query_result.ard = True
            query_result.ard_path = ard_path
            ses.commit()
        ses.close()
        logger.debug("Closed the database session.")

    def get_scns_ard(self, granule=None):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        logger.debug("Perform query to find scene.")
        if granule is None:
            query_result = ses.query(Sen2Process).filter(Sen2Process.download == True,
                                                         Sen2Process.ard == False).all()
        else:
            query_result = ses.query(Sen2Process).filter(Sen2Process.granule == granule,
                                                         Sen2Process.download == True,
                                                         Sen2Process.ard == False).all()
        scns = list()
        for scn in query_result:
            scns.append(scn)
        ses.close()
        logger.debug("Closed the database session.")
        return scns

    def is_scn_ard(self, product_id):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        logger.debug("Perform query to find scene.")
        query_result = ses.query(Sen2Process).filter(Sen2Process.product_id == product_id).one_or_none()
        ses.close()
        logger.debug("Closed the database session.")
        arded = False
        if query_result is not None:
            if query_result.ard_path is not None:
                arded = True
            else:
                arded = query_result.ard
        return arded

    def reset_all_scn(self, product_id, delpath=False):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        logger.debug("Perform query to find scene.")
        query_result = ses.query(Sen2Process).filter(Sen2Process.product_id == product_id).one_or_none()
        if query_result is not None:
            logger.debug("Resetting ARD and Download fields for {}.".format(product_id))
            query_result.ard = False
            if delpath and query_result.ard_path is not None and os.path.exists(query_result.ard_path):
                shutil.rmtree(query_result.ard_path)
            query_result.ard_path = None
            if delpath and query_result.download_path is not None and os.path.exists(query_result.download_path):
                shutil.rmtree(query_result.download_path)
            query_result.download = False
            query_result.download_path = None
            ses.commit()
            logger.debug("Reset ARD and Download fields for {}.".format(product_id))
        else:
            logger.info("Failed to reset {} was not found.".format(product_id))
        ses.close()
        logger.debug("Closed the database session.")

    def reset_ard_scn(self, product_id, delpath=False):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        logger.debug("Perform query to find scene.")
        query_result = ses.query(Sen2Process).filter(Sen2Process.product_id == product_id).one_or_none()
        if query_result is not None:
            logger.debug("Resetting ARD fields for {}.".format(product_id))
            query_result.ard = False
            if delpath and query_result.ard_path is not None and os.path.exists(query_result.ard_path):
                shutil.rmtree(query_result.ard_path)
            query_result.ard_path = None
            ses.commit()
            logger.debug("Reset ARD fields for {}.".format(product_id))
        else:
            logger.info("Failed to reset {} was not found.".format(product_id))
        ses.close()
        logger.debug("Closed the database session.")

    def reset_dwnld_scn(self, product_id, delpath=False):
        logger.debug("Creating Database Engine.")
        db_engine = sqlalchemy.create_engine(self.sqlite_db_conn, pool_pre_ping=True)
        logger.debug("Creating Database Session.")
        session_sqlalc = sqlalchemy.orm.sessionmaker(bind=db_engine)
        ses = session_sqlalc()
        logger.debug("Created Database Engine and Session.")

        logger.debug("Perform query to find scene.")
        query_result = ses.query(Sen2Process).filter(Sen2Process.product_id == product_id).one_or_none()
        if query_result is not None:
            logger.debug("Resetting Download fields for {}.".format(product_id))
            query_result.download = False
            if delpath and query_result.download_path is not None and os.path.exists(query_result.download_path):
                shutil.rmtree(query_result.download_path)
            query_result.download_path = None
            ses.commit()
            logger.debug("Reset Download fields for {}.".format(product_id))
        else:
            logger.info("Failed to reset {} was not found.".format(product_id))
        ses.close()
        logger.debug("Closed the database session.")




