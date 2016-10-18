"""update-sf133-unique-index

Revision ID: a0a4f1ef56ae
Revises: e3d7eb4d44f0
Create Date: 2016-08-10 17:46:51.920340

"""

# revision identifiers, used by Alembic.
revision = 'a0a4f1ef56ae'
down_revision = 'e3d7eb4d44f0'
branch_labels = None
depends_on = None

from alembic import op


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_data_broker():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_sf_133_tas', table_name='sf_133')
    op.create_index('ix_sf_133_tas', 'sf_133', ['tas', 'period', 'line'], unique=True)
    ### end Alembic commands ###


def downgrade_data_broker():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('ix_sf_133_tas', table_name='sf_133')
    op.create_index('ix_sf_133_tas', 'sf_133', ['tas', 'line'], unique=True)
    ### end Alembic commands ###

