import sqlite3
from collections import namedtuple

ROOT_ID_SQL = 'SELECT id FROM nodes WHERE name IS NULL AND type == "folder"'
CHILDREN_OF_SQL = """SELECT n.name FROM nodes n
                JOIN parentage p ON n.id = p.child
                WHERE p.parent = (?) AND n.status == 'AVAILABLE'
                ORDER BY n.name"""
CHILD_OF_SQL = """SELECT n.* FROM nodes n
                  JOIN parentage p ON n.id = p.child
                  WHERE n.name = (?) AND p.parent = (?) AND n.status == 'AVAILABLE'"""
NODE_BY_ID_SQL = 'SELECT * FROM nodes WHERE id = (?)'

Node = namedtuple('Node',
                  ['id', 'type', 'name', 'description', 'created', 'modified', 'updated', 'status'])


def namedtuple_factory(cursor: sqlite3.Cursor, row: sqlite3.Row):
    """Namedtuple row factory for sqlite3"""
    fields = [col[0] for col in cursor.description]
    Row = namedtuple("Row", fields)
    return Row(*row)


def create_conn(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(path, timeout=60)
    # c.row_factory = namedtuple_factory
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
        if r[1] == 'folder':
            parent = r[0]
            continue
        else:
            return


def children(conn: sqlite3.Connection, id: str) -> sqlite3.Cursor:
    c = conn.cursor()
    c.execute(CHILDREN_OF_SQL, [id])
    kids = []
    row = c.fetchone()
    while row:
        kids.append(row[0])
        row = c.fetchone()
    c.close()
    return kids


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
        return Node(*node)

    def children(self, folder_id):
        conn = create_conn(self.db_path)
        kids = children(conn, folder_id)
        conn.close()
        return kids
