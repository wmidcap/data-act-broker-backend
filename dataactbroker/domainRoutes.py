from functools import wraps
from flask import g

from dataactcore.interfaces.db import GlobalDB
from dataactcore.models.domainModels import SubTierAgency
from dataactcore.models.lookups import PERMISSION_SHORT_DICT
from dataactcore.utils.jsonResponse import JsonResponse
from dataactcore.utils.statusCode import StatusCode


WRITE_PERM = PERMISSION_SHORT_DICT['w']
FABS_PERM = PERMISSION_SHORT_DICT['f']


# Add the file submission route
def add_domain_routes(app):
    """ Create routes related to domain values for flask app """

    @app.route("/v1/list_agencies/", methods=["GET"])
    @get_dabs_sub_tier_agencies
    def list_agencies(sub_tier_agencies):
        """ List all CGAC and FREC Agencies user has DABS permissions for
            Args:
            sub_tier_agencies - List of all SubTierAgencies generated by the get_dabs_sub_tier_agencies decorator,
                required to list only sub_tier_agencies that user has DABS permissions for
        """
        cgac_list, frec_list = [], []
        for sub_tier in sub_tier_agencies:
            if sub_tier.is_frec:
                frec_list.append({'agency_name': sub_tier.frec.agency_name, 'frec_code': sub_tier.frec.frec_code,
                                  'priority': sub_tier.priority})
            else:
                cgac_list.append({'agency_name': sub_tier.cgac.agency_name, 'cgac_code': sub_tier.cgac.cgac_code,
                                  'priority': sub_tier.priority})

        return JsonResponse.create(StatusCode.OK, {'cgac_agency_list': cgac_list, 'frec_agency_list': frec_list})

    @app.route("/v1/list_all_agencies/", methods=["GET"])
    def list_all_agencies():
        """ List all CGAC and FREC Agencies
        """
        sess = GlobalDB.db().session
        agency_list, shared_list = [], []

        # distinct SubTierAgency FRECs with a True is_frec
        for agency in sess.query(SubTierAgency).filter_by(is_frec=True).distinct(SubTierAgency.frec_id).all():
            shared_list.append({'agency_name': agency.frec.agency_name, 'frec_code': agency.frec.frec_code,
                                'priority': agency.priority})

        # distinct SubTierAgency CGACs with a False is_frec
        for agency in sess.query(SubTierAgency).filter_by(is_frec=False).distinct(SubTierAgency.cgac_id).all():
            agency_list.append({'agency_name': agency.cgac.agency_name, 'cgac_code': agency.cgac.cgac_code,
                                'priority': agency.priority})

        return JsonResponse.create(StatusCode.OK, {'agency_list': agency_list, 'shared_agency_list': shared_list})

    @app.route("/v1/list_sub_tier_agencies/", methods=["GET"])
    @get_fabs_sub_tier_agencies
    def list_sub_tier_agencies(sub_tier_agencies):
        """ List all Sub-Tier Agencies user has FABS permissions for
            Args:
            sub_tier_agencies - List of all SubTierAgencies generated by the get_fabs_sub_tier_agencies decorator,
                required to list only sub_tier_agencies that user has FABS permissions for
        """
        agencies = []
        for sub_tier in sub_tier_agencies:
            agency_name = sub_tier.frec.agency_name if sub_tier.is_frec else sub_tier.cgac.agency_name
            agencies.append({'agency_name': '{}: {}'.format(agency_name, sub_tier.sub_tier_agency_name),
                             'agency_code': sub_tier.sub_tier_agency_code, 'priority': sub_tier.priority})

        return JsonResponse.create(StatusCode.OK, {'sub_tier_agency_list': agencies})


def get_dabs_sub_tier_agencies(fn):
    """ Decorator which provides a list of all SubTierAgencies the user has DABS permissions for. The function should
    have a sub_tier_agencies parameter as its first argument. """
    @wraps(fn)
    def wrapped(*args, **kwargs):
        sub_tier_agencies = []
        if g.user is None:
            sub_tier_agencies = []
        else:
            # create list of affiliations
            cgac_ids, frec_ids = [], []
            for affil in g.user.affiliations:
                if affil.permission_type_id >= WRITE_PERM and affil.permission_type_id != FABS_PERM:
                    if affil.frec:
                        frec_ids.append(affil.frec.frec_id)
                    else:
                        cgac_ids.append(affil.cgac.cgac_id)

            # generate SubTierAgencies based on permissions
            cgac_sub_tiers, frec_sub_tiers = get_sub_tiers_from_perms(g.user.website_admin, cgac_ids, frec_ids)

            # filter out copies of top-tier agencies
            cgac_sub_tier_agencies = cgac_sub_tiers.distinct(SubTierAgency.cgac_id).all()
            frec_sub_tier_agencies = frec_sub_tiers.distinct(SubTierAgency.frec_id).all()

            sub_tier_agencies = cgac_sub_tier_agencies + frec_sub_tier_agencies

        return fn(sub_tier_agencies, *args, **kwargs)
    return wrapped


def get_fabs_sub_tier_agencies(fn):
    """ Decorator which provides a list of all SubTierAgencies the user has FABS permissions for. The function should
    have a sub_tier_agencies parameter as its first argument. """
    @wraps(fn)
    def wrapped(*args, **kwargs):
        sub_tier_agencies = []
        if g.user is not None:
            # create list of affiliations
            cgac_ids, frec_ids = [], []
            for affil in g.user.affiliations:
                if affil.permission_type_id == FABS_PERM:
                    if affil.frec:
                        frec_ids.append(affil.frec.frec_id)
                    else:
                        cgac_ids.append(affil.cgac.cgac_id)

            # generate SubTierAgencies based on permissions
            cgac_sub_tiers, frec_sub_tiers = get_sub_tiers_from_perms(g.user.website_admin, cgac_ids, frec_ids)

            sub_tier_agencies = cgac_sub_tiers.all() + frec_sub_tiers.all()

        return fn(sub_tier_agencies, *args, **kwargs)
    return wrapped


def get_sub_tiers_from_perms(is_admin, cgac_affil_ids, frec_affil_ids):
    sess = GlobalDB.db().session
    cgac_sub_tier_agencies = sess.query(SubTierAgency).filter(SubTierAgency.is_frec.is_(False))
    frec_sub_tier_agencies = sess.query(SubTierAgency).filter(SubTierAgency.is_frec.is_(True))

    # filter by user affiliations if user is not admin
    if not is_admin:
        cgac_sub_tier_agencies = cgac_sub_tier_agencies.filter(SubTierAgency.cgac_id.in_(cgac_affil_ids))
        frec_sub_tier_agencies = frec_sub_tier_agencies.filter(SubTierAgency.frec_id.in_(frec_affil_ids))

    return cgac_sub_tier_agencies, frec_sub_tier_agencies
