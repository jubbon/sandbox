#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlalchemy as sa
from aiopg.sa import create_engine

DATABASE = {
    'drivername':   'postgres',
    'host':         '127.0.0.1',
    'port':         '5432',
    'database':     'postgres',
    'username':     'postgres',
    'password':     'postgres'
}

metadata = sa.MetaData()

users = sa.Table('users', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('name', sa.String(255)),
    sa.Column('birthday', sa.DateTime)
)

emails = sa.Table('emails', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('user_id', None, sa.ForeignKey('users.id')),
    sa.Column('email', sa.String(255), nullable=False),
    sa.Column('private', sa.Boolean, nullable=False)
)

async def create_tables(conn):
    '''
    Create all tables if they does not exist yet
    '''
    from sqlalchemy import create_engine as sa_create_engine
    from sqlalchemy.engine.url import URL
    engine = sa_create_engine(str(URL(**DATABASE)))
    metadata.create_all(engine)
    engine.dispose()


async def count(conn):
    '''
    Calculate row count
    '''
    users_count = await conn.scalar(users.count())
    emails_count = await conn.scalar(emails.count())
    print('Total users: {}, total emails: {}'.format(users_count, emails_count))


async def dump(conn):
    '''
    Dump row form tables
    '''
    print('Users:')
    async for row in conn.execute(users.select()):
        print("{}: {}".format(row.name, row.birthday))

    print('Emails:')
    async for row in conn.execute(emails.select()):
        print("{}: {}".format(row.user_id, row.email))


async def go():
    '''
    '''
    DB = dict(
        host=DATABASE['host'],
        port=DATABASE['port'],
        user=DATABASE['username'],
        password=DATABASE['password']
    )
    engine = await create_engine(**DB)
    async with engine:
        async with engine.acquire() as conn:
            await create_tables(conn)
            await count(conn)
            await dump(conn)

def main():
    '''
    '''
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(go())


if __name__ == "__main__":
    main()
