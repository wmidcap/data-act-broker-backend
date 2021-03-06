"""Cascade UserAffiliation deletes

Revision ID: 5de499ab5b62
Revises: 14f51f27a106
Create Date: 2016-12-13 00:21:39.842218

"""

# revision identifiers, used by Alembic.
revision = '5de499ab5b62'
down_revision = '14f51f27a106'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('user_affiliation_user_fk', 'user_affiliation', type_='foreignkey')
    op.drop_constraint('user_affiliation_cgac_fk', 'user_affiliation', type_='foreignkey')
    op.create_foreign_key('user_affiliation_user_fk', 'user_affiliation', 'users', ['user_id'], ['user_id'], ondelete='CASCADE')
    op.create_foreign_key('user_affiliation_cgac_fk', 'user_affiliation', 'cgac', ['cgac_id'], ['cgac_id'], ondelete='CASCADE')
    ### end Alembic commands ###


def downgrade_data_broker():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('user_affiliation_cgac_fk', 'user_affiliation', type_='foreignkey')
    op.drop_constraint('user_affiliation_user_fk', 'user_affiliation', type_='foreignkey')
    op.create_foreign_key('user_affiliation_cgac_fk', 'user_affiliation', 'cgac', ['cgac_id'], ['cgac_id'])
    op.create_foreign_key('user_affiliation_user_fk', 'user_affiliation', 'users', ['user_id'], ['user_id'])
    ### end Alembic commands ###

