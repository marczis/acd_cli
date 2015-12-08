import sqlite3
from datetime import datetime


def datetime_from_string(dt: str) -> datetime:
    try:
        dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S.%f+00:00')
    except ValueError:
        dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S+00:00')
    return dt


ROOT_ID_SQL = 'SELECT id FROM nodes WHERE name IS NULL AND type == "folder"'
CHILDREN_OF_SQL = """SELECT n.name FROM nodes n
                JOIN parentage p ON n.id = p.child
                WHERE p.parent = (?) AND n.status == 'AVAILABLE'
                ORDER BY n.name"""
NUM_CHILDREN_SQL = """SELECT COUNT(n.id) FROM nodes n
                    JOIN parentage p ON n.id = p.child
                    WHERE p.parent = (?) AND n.status == 'AVAILABLE'"""
NUM_PARENTS_SQL = """SELECT COUNT(n.id) FROM nodes n
                    JOIN parentage p ON n.id = p.parent
                    WHERE p.child = (?) AND n.status == 'AVAILABLE'"""
CHILD_OF_SQL = """SELECT n.*, f.md5 AS md5, f.size AS size FROM nodes n
                  JOIN parentage p ON n.id = p.child
                  LEFT OUTER JOIN files f ON n.id = f.id
                  WHERE n.name = (?) AND p.parent = (?) AND n.status == 'AVAILABLE'"""
NODE_BY_ID_SQL = 'SELECT * FROM nodes WHERE id = (?)'


class Node(object):
    def __init__(self, row):
        self.id = row['id']
        self.type = row['type']
        self.name = row['name']
        self.description = row['description']
        self.cre = row['created']
        self.mod = row['modified']
        self.updated = row['updated']
        self.status = row['status']

        try:
            self.md5 = row['md5']
        except IndexError:
            self.md5 = None
        try:
            self.size = row['size']
        except IndexError:
            self.size = 0

    @property
    def is_folder(self):
        return self.type == 'folder'

    @property
    def is_file(self):
        return self.type == 'file'

    @property
    def created(self):
        return datetime_from_string(self.cre)

    @property
    def modified(self):
        return datetime_from_string(self.mod)


def create_conn(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(path, timeout=60)
    # c.row_factory = namedtuple_factory
    c.row_factory = sqlite3.Row # allow dict-like access on rows with col name
    return c


def get_root_id(conn: sqlite3.Connection) -> str:
    c = conn.cursor()
    c.execute(ROOT_ID_SQL)
    first_id = c.fetchone()[0]

    if c.fetchone():
        c.close()
        raise Exception
    c.close()

    return first_id


def resolve(conn: sqlite3.Connection, path: str, root_id: str) -> tuple:
    segments = list(filter(bool, path.split('/')))
    if not segments:
        c = conn.cursor()
        c.execute(NODE_BY_ID_SQL, [root_id])
        r = c.fetchone()
        c.close()
        return r

    parent = root_id
    for i, segment in enumerate(segments):
        c = conn.cursor()
        c.execute(CHILD_OF_SQL, [segment, parent])
        r = c.fetchone()
        c.close()
        if not r:
            return
        if i + 1 == segments.__len__():
            return r
        if r['type'] == 'folder':
            parent = r[0]
            continue
        else:
            return


def list_children(conn: sqlite3.Connection, id: str) -> list:
    c = conn.cursor()
    c.execute(CHILDREN_OF_SQL, [id])
    kids = []
    row = c.fetchone()
    while row:
        kids.append(row[0])
        row = c.fetchone()
    c.close()
    return kids


def num_children(conn: sqlite3.Connection, id: str) -> int:
    c = conn.cursor()
    c.execute(NUM_CHILDREN_SQL, [id])
    num = c.fetchone()[0]
    c.close()
    return num


def num_parents(conn: sqlite3.Connection, id: str) -> int:
    c = conn.cursor()
    c.execute(NUM_PARENTS_SQL, [id])
    num = c.fetchone()[0]
    c.close()
    return num


class RawQuery(object):
    def __init__(self, path: str):
        self.db_path = path
        conn = create_conn(self.db_path)
        self.root_id = get_root_id(conn)
        conn.close

    def resolve(self, path):
        conn = create_conn(self.db_path)
        node = resolve(conn, path, self.root_id)
        conn.close()
        if not node:
            return node
        return Node(node)

    def children(self, folder_id):
        conn = create_conn(self.db_path)
        kids = list_children(conn, folder_id)
        conn.close()
        return kids

    def num_children(self, folder_id):
        conn = create_conn(self.db_path)
        num = num_children(conn, folder_id)
        conn.close()
        return num

    def num_parents(self, node_id):
        conn = create_conn(self.db_path)
        num = num_parents(conn, node_id)
        conn.close()
        return num
