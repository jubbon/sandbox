#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime

import sqlalchemy as sa
from aiopg.sa import create_engine

DATABASE = {
    'drivername': 'postgres',
    'host':     '127.0.0.1',
    'port': '5432',
    'database': 'postgres',
    'username':  'postgres',
    'password': 'postgres'
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

async def insert(**kwargs):
    '''
    '''
    assert 'name' in kwargs
    assert 'birthday' in kwargs

    DB = dict(
        host=DATABASE['host'],
        port=DATABASE['port'],
        user=DATABASE['username'],
        password=DATABASE['password']
    )
    async with create_engine(**DB) as engine:
        async with engine.acquire() as conn:
            await conn.execute(users.insert().values(**kwargs))

def main():
    '''
    '''
    # Create all tables if they does not exist yet
    from sqlalchemy import create_engine as sa_create_engine
    from sqlalchemy.engine.url import URL
    engine = sa_create_engine(str(URL(**DATABASE)))
    metadata.create_all(engine)
    engine.dispose()

    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        insert(
            name='Ivan Petrov',
            birthday=datetime(1981, 5, 1)
        )
    )

if __name__ == "__main__":
    main()
