import tempfile
from collections import namedtuple

import pytz
import singer
from bingads import AuthorizationData, OAuthDesktopMobileAuthCodeGrant
from bingads.v13.bulk import (
    BulkAccount,
    BulkAdGroup,
    BulkCampaign,
    BulkFileReader,
    BulkServiceManager,
    DownloadParameters
)
from singer import (
    metadata,
    metrics,
    strftime,
)

from tap_bing_ads.clients import log_service_call
from tap_bing_ads.transform import sobject_to_dict


LOGGER = singer.get_logger()


# class InvalidDateRangeEnd(Exception):
#     pass


def get_bookmark(state, stream, account_id, key, default=None):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, {})
        .get(account_id, {})
        .get(key, default)
    )


def write_bookmark(state, stream, account_id, key, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    if stream not in state['bookmarks']:
        state['bookmarks'][stream] = {}
    if account_id not in state['bookmarks'][stream]:
        state['bookmarks'][stream][account_id] = {}
    state['bookmarks'][stream][account_id][key] = value
    LOGGER.info('Write state for Stream: {}, Account: {}, key: {}, value: {}'.format(
        stream, account_id, key, value))
    singer.write_state(state)


class BulkServiceWrapper(object):
    def __init__(self, config, account_id):
        self._account_id = account_id

        authentication = OAuthDesktopMobileAuthCodeGrant(
            client_id=config['oauth_client_id'])
        authentication.request_oauth_tokens_by_refresh_token(
            config['refresh_token'])

        authorization_data = AuthorizationData(
            account_id=self._account_id,
            customer_id=config['customer_id'],
            developer_token=config['developer_token'],
            authentication=authentication)

        self._bulk_service = BulkServiceManager(
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=5000,
            environment=config.get('environment', 'production')
        )

        self._temp_path = None

    @property
    def temp_path(self):
        if self._temp_path is None:
            self._temp_path = tempfile.mkdtemp()
            # logging where the output dir is so we can find if when debugging
            LOGGER.info(
                "Temp directory for bulk download: %s", self._temp_path)
        return self._temp_path

    def entities_generator(self, entities, last_sync_time):
        download_parameters = DownloadParameters(
            campaign_ids=None,
            data_scope=['EntityData'],
            download_entities=entities,
            file_type='Csv',
            last_sync_time_in_utc=last_sync_time,
            result_file_directory=self.temp_path,
            result_file_name='temp.csv',
            overwrite_result_file = True
        )
        result_file_path = log_service_call(self._bulk_service.download_file, self._account_id)(download_parameters)
        LOGGER.info(
            "Temp file for bulk download: %s", result_file_path)

        with BulkFileReader(file_path=result_file_path) as reader:
            for entity in reader:
                yield entity


# def sync_core_objects(account_id, selected_streams):
#     client = create_sdk_client('CampaignManagementService', account_id)

#     LOGGER.info('Syncing Campaigns for Account: {}'.format(account_id))
#     campaign_ids = sync_campaigns(client, account_id, selected_streams)

#     if campaign_ids and ('ad_groups' in selected_streams or 'ads' in selected_streams):
#         ad_group_ids = sync_ad_groups(client, account_id, campaign_ids, selected_streams)
#         if 'ads' in selected_streams:
#             LOGGER.info('Syncing Ads for Account: {}'.format(account_id))
#             sync_ads(client, selected_streams, ad_group_ids)


CoreObjectMetadata = namedtuple('CoreObjectMetadata', ['DownloadEntity', 'entity_attr', 'transform_function'])


def sync_core_objects(account_id, selected_streams, config, state):
    bulk_service = BulkServiceWrapper(config, account_id)

    if 'campaigns' in selected_streams:
        LOGGER.info('Syncing Campaigns for Account: {}'.format(account_id))
        sync_core_object(
            CoreObjectMetadata('Campaigns', 'campaign', transform_campaign),
            bulk_service,
            account_id,
            selected_streams.get('campaigns'),
            state,
            config['bulk_start_date'])

    if 'ad_groups' in selected_streams:
        LOGGER.info('Syncing Ad Groups for Account: {}'.format(account_id))
        sync_core_object(
            CoreObjectMetadata('AdGroups', 'ad_group', transform_ad_group),
            bulk_service,
            account_id,
            selected_streams.get('ad_groups'),
            state,
            config['bulk_start_date'])

    if 'ads' in selected_streams:
        LOGGER.info('Syncing Ads for Account: {}'.format(account_id))
        sync_core_object(
            CoreObjectMetadata('Ads', 'ad', transform_ad),
            bulk_service,
            account_id,
            selected_streams.get('ads'),
            state,
            config['bulk_start_date'])

    # if 'keywords' in selected_streams:
    #     pass


def sync_core_object(object_metadata, bulk_service, account_id, stream, state, start_date=None):
    last_sync_time = get_bookmark(
        state, stream.tap_stream_id, account_id, 'last_sync_time')
    if not last_sync_time:
        last_sync_time = start_date

    LOGGER.info('Last sync time Stream: {}, Account: {}: {}'.format(
        stream.tap_stream_id, account_id, last_sync_time))

    core_objects = []

    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)
    singer.write_schema(stream.tap_stream_id, schema, ['Id'])

    with metrics.record_counter(stream.tap_stream_id) as counter:
        for entity in bulk_service.entities_generator([object_metadata.DownloadEntity], last_sync_time):
            if isinstance(entity, BulkAccount):
                new_last_sync_time = pytz.utc.localize(entity.sync_time)
            elif hasattr(entity, object_metadata.entity_attr):
                entity = object_metadata.transform_function(entity)
                core_object = singer.transform(
                    sobject_to_dict(getattr(entity, object_metadata.entity_attr)),
                    schema,
                    stream_metadata)
                core_objects.append(core_object)

                singer.write_record(
                    stream.tap_stream_id,
                    core_object
                )
                counter.increment()

    write_bookmark(
        state,
        stream.tap_stream_id,
        account_id,
        'last_sync_time',
        strftime(new_last_sync_time))

    return map(lambda x: x['Id'], core_objects)


def transform_campaign(entity):
    """
    Transform Campaigns returned from Bulk service to expected schema
    """
    # 'CampaignType' is a singleton list in the Bulk service
    entity.campaign.CampaignType = (
        entity.campaign.CampaignType[0]
        if entity.campaign.CampaignType else None
    )
    return entity


def transform_ad_group(entity):
    return entity


def transform_ad(entity):
    return entity


def sync_ads(client, selected_streams, ad_group_ids):
    for ad_group_id in ad_group_ids:
        response = client.GetAdsByAdGroupId(
            AdGroupId=ad_group_id,
            AdTypes={
                'AdType': [
                    'AppInstall',
                    'DynamicSearch',
                    'ExpandedText',
                    'Product',
                    'Text',
                    'Image'
                ]
            })
        response_dict = sobject_to_dict(response)

        if 'Ad' in response_dict:
            selected_fields = get_selected_fields(selected_streams['ads'])
            singer.write_schema('ads', get_core_schema(client, 'Ad'), ['Id'])
            with metrics.record_counter('ads') as counter:
                ads = response_dict['Ad']
                singer.write_records('ads', filter_selected_fields_many(selected_fields, ads))
                counter.increment(len(ads))
