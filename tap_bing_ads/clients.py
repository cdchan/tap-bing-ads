import singer
import suds
from singer import metrics


LOGGER = singer.get_logger()


class InvalidDateRangeEnd(Exception):
    pass


class LastSyncTimeTooOld(Exception):
    pass


def log_service_call(service_method, account_id):
    def wrapper(*args, **kwargs):
        if hasattr(service_method, 'name'):
            service_method_name = getattr(service_method, 'name')
        else:
            service_method_name = service_method.__name__

        log_args = list(map(lambda arg: str(arg).replace('\n', '\\n'), args)) + \
                   list(map(lambda kv: '{}={}'.format(*kv), kwargs.items()))
        LOGGER.info('Calling: {}({}) for account: {}'.format(
            service_method_name,
            ','.join(log_args),
            account_id))
        with metrics.http_request_timer(service_method_name):
            try:
                return service_method(*args, **kwargs)
            except suds.WebFault as e:
                LOGGER.info('Caught exception for account: {}'.format(account_id))
                if hasattr(e.fault.detail, 'ApiFaultDetail'):
                    # The Web fault structure is heavily nested. This is to be sure we catch the error we want.
                    operation_errors = e.fault.detail.ApiFaultDetail.OperationErrors

                    invalid_date_range_end_errors = [
                        oe for (_, oe) in operation_errors
                        if oe.ErrorCode == 'InvalidCustomDateRangeEnd'
                    ]
                    if any(invalid_date_range_end_errors):
                        raise InvalidDateRangeEnd(invalid_date_range_end_errors) from e

                    sync_time_too_old_errors = [
                        oe for (_, oe) in operation_errors
                        if oe.ErrorCode == 'CampaignServiceLastSyncTimeTooOld'
                    ]
                    if any(sync_time_too_old_errors):
                        raise LastSyncTimeTooOld(sync_time_too_old_errors) from e

                    raise Exception(operation_errors) from e
                if hasattr(e.fault.detail, 'AdApiFaultDetail'):
                    raise Exception(e.fault.detail.AdApiFaultDetail.Errors) from e

    return wrapper
