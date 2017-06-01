"""Roll back award_procurement updates

Revision ID: 821a7f4694f0
Revises: cec02816fc8a
Create Date: 2017-05-31 15:06:53.681793

"""

# revision identifiers, used by Alembic.
revision = '821a7f4694f0'
down_revision = 'cec02816fc8a'
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
    op.drop_column('award_procurement', 'referenced_mult_or_single')
    op.drop_column('award_procurement', 'place_of_perform_city_name')
    op.drop_column('award_procurement', 'legal_entity_state_descrip')
    op.drop_column('award_procurement', 'referenced_idv_type')
    ### end Alembic commands ###


def downgrade_data_broker():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('award_procurement', sa.Column('referenced_idv_type', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('award_procurement', sa.Column('legal_entity_state_descrip', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('award_procurement', sa.Column('place_of_perform_city_name', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('award_procurement', sa.Column('referenced_mult_or_single', sa.TEXT(), autoincrement=False, nullable=True))
    ### end Alembic commands ###
