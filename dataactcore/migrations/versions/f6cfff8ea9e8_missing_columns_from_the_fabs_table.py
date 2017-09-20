"""Missing columns from the FABS table

Revision ID: f6cfff8ea9e8
Revises: cd1025ac9399
Create Date: 2017-09-20 12:00:37.357492

"""

# revision identifiers, used by Alembic.
revision = 'f6cfff8ea9e8'
down_revision = 'cd1025ac9399'
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
    op.add_column('detached_award_financial_assistance', sa.Column('funding_agency_name', sa.Text(), nullable=True))
    op.add_column('detached_award_financial_assistance', sa.Column('funding_office_name', sa.Text(), nullable=True))
    ### end Alembic commands ###


def downgrade_data_broker():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('detached_award_financial_assistance', 'funding_office_name')
    op.drop_column('detached_award_financial_assistance', 'funding_agency_name')
    ### end Alembic commands ###

