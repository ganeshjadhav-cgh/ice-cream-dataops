from datetime import datetime, timedelta, timezone
from itertools import islice
from timeit import default_timer

from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipelineRun
from cognite.client.exceptions import CogniteAPIError
from cognite.client.data_classes.data_modeling import DirectRelationReference, NodeId, ViewId
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteAsset, CogniteTimeSeries
from cognite.client.data_classes.filters import Prefix, ContainsAny, Equals

from ice_cream_factory_api import IceCreamFactoryAPI

from cognite.client.config import global_config
global_config.disable_pypi_version_check = True

from itertools import islice


def batcher(iterable, batch_size):
    iterator = iter(iterable)
    while batch := list(islice(iterator, batch_size)):
        yield batch


def get_time_series_for_site(client: CogniteClient, site):
    this_site = site.lower()
    sub_tree_root = client.data_modeling.instances.retrieve_nodes(
        NodeId("icapi_dm_space", this_site),
        node_cls=CogniteAsset
    )

    if not sub_tree_root:
        print(
            f"----No CogniteAssets in CDF for {site}!----\n"
            f"    Run the 'Create Cognite Asset Hierarchy' transformation!"
        )
        return []

    if not sub_tree_root.path:
        print(
            f"----CogniteAsset '{site}' has no path property!----\n"
            f"    Falling back to subtree traversal using parent relationships.\n"
            f"    Run the 'Create Cognite Asset Hierarchy' transformation if this still fails!"
        )

        def collect_subtree_assets(root_asset):
            asset_queue = [root_asset]
            collected = [root_asset]
            while asset_queue:
                current_asset = asset_queue.pop(0)
                try:
                    children = client.data_modeling.instances.list(
                        instance_type=CogniteAsset,
                        space=current_asset.space,
                        filter=Equals(
                            property=["cdf_cdm", "CogniteAsset/v1", "parent"],
                            value=DirectRelationReference.load(
                                {"space": current_asset.space, "externalId": current_asset.external_id}
                            ),
                        ),
                        limit=None,
                    )
                except Exception as e:
                    print(
                        f"Failed loading child assets for site {site} root {current_asset.external_id}: {e}"
                    )
                    return collected

                if children:
                    collected.extend(children)
                    asset_queue.extend(children)
            return collected

        sub_tree_nodes = collect_subtree_assets(sub_tree_root)
        if len(sub_tree_nodes) <= 1:
            print(
                f"----No descendant CogniteAssets found under {site}!----\n"
                f"    Run the 'Create Cognite Asset Hierarchy' transformation!"
            )
            return []

        value_list = [{"space": node.space, "externalId": node.external_id} for node in sub_tree_nodes]
        time_series_batches = []
        for batch in batcher(value_list, 20):
            try:
                batch_result = client.data_modeling.instances.search(
                    view=ViewId("cdf_cdm", "CogniteTimeSeries", "v1"),
                    instance_type=CogniteTimeSeries,
                    filter=ContainsAny(property=["cdf_cdm", "CogniteTimeSeries/v1", "assets"], values=batch),
                    limit=None
                )
            except Exception as e:
                print(f"Failed searching CogniteTimeSeries for site {site}: {e}")
                continue
            time_series_batches.append(batch_result)

        # Combine list of batch results into a single NodeList
        time_series = [node for nodelist in time_series_batches for node in nodelist]

        if not time_series:
            print(
                f"----No CogniteTimeSeries in CDF for {site}!----\n"
                f"    Run the 'Contextualize Timeseries and Assets' transformation!"
            )

        time_series = [
            item for item in time_series
            if any(substring in item.external_id for substring in ["planned_status", "good"])
        ]

        return time_series

    try:
        sub_tree_nodes = client.data_modeling.instances.list(
            instance_type=CogniteAsset,
            space=sub_tree_root.space,
            filter=Prefix(
                property=["cdf_cdm", "CogniteAsset/v1", "path"],
                value=sub_tree_root.path
            ),
            limit=None
        )
    except Exception as e:
        print(
            f"Failed to list CogniteAssets under site {site} "
            f"({sub_tree_root.space}/{sub_tree_root.external_id}): {e}"
        )
        return []

    if not sub_tree_nodes:
        print(
            f"----No CogniteTimeSeries in CDF for {site}!----\n"
            f"    Run the 'Contextualize Timeseries and Assets' transformation!"
        )
        return []

    value_list = [{"space": node.space, "externalId": node.external_id} for node in sub_tree_nodes]

    time_series_batches = []
    for batch in batcher(value_list, 20):
        try:
            batch_result = client.data_modeling.instances.search(
                view=ViewId("cdf_cdm", "CogniteTimeSeries", "v1"),
                instance_type=CogniteTimeSeries,
                filter=ContainsAny(property=["cdf_cdm", "CogniteTimeSeries/v1", "assets"], values=batch),
                limit=None
            )
        except Exception as e:
            print(f"Failed searching CogniteTimeSeries for site {site}: {e}")
            continue
        time_series_batches.append(batch_result)

    # Combine list of batch results into a single NodeList
    time_series = [node for nodelist in time_series_batches for node in nodelist]

    if not time_series:
        print("No CogniteTimeSeries in the CogniteCore Data Model (cdf_cdm Space)")

    time_series = [
        item for item in time_series
        if any(substring in item.external_id for substring in ["planned_status", "good"])
    ]

    return time_series


def report_ext_pipe(client: CogniteClient, status, message=None):
    if message is not None and not isinstance(message, str):
        message = str(message)

    ext_pipe_run = ExtractionPipelineRun(
        extpipe_external_id="ep_icapi_datapoints",
        status=status,
        message=message
    )

    try:
        client.extraction_pipelines.runs.create(run=ext_pipe_run)
    except CogniteAPIError as e:
        print(f"Unable to report extraction run status '{status}': {e}")
    except Exception as e:
        print(f"Unable to report extraction run status '{status}': {e}")

def handle(client: CogniteClient = None, data=None):
    report_ext_pipe(client, "seen")
    
    sites = None
    backfill = None
    hours = None
    max_hours = 336

    if data:
        sites = data.get("sites")
        backfill = data.get("backfill")
        hours = data.get("hours")

        if hours and hours > max_hours:
            print(f"{hours} > {max_hours}! The Ice Cream API can't serve more than {max_hours} hours of datapoints, setting hours to max")
            hours = max_hours

    all_sites = [
        "Houston",
        "Oslo",
        "Kuala_Lumpur",
        "Hannover",
        "Nuremberg",
        "Marseille",
        "Sao_Paulo",
        "Chicago",
        "Rotterdam",
        "London",
    ]

    sites = sites or all_sites
    backfill = backfill or True
    hours = hours or max_hours

    now = datetime.now(timezone.utc).timestamp() * 1000
    increment = timedelta(hours=hours).total_seconds() * 1000

    ice_cream_api = IceCreamFactoryAPI(base_url="https://ice-cream-factory.inso-internal.cognite.ai")

    try:
        for site in sites:
            print(f"Getting Data Points for {site}")
            big_start = default_timer()

            time_series = get_time_series_for_site(client, site)

            latest_dps = {
                dp.external_id: dp.timestamp
                for dp in client.time_series.data.retrieve_latest(
                    external_id=[ts.external_id for ts in time_series],
                    ignore_unknown_ids=True
                )
            } if not backfill else None

            to_insert = []
            for ts in time_series:
                # figure out the window of datapoints to pull for this Time Series
                latest = latest_dps[ts.external_id][0] if not backfill and latest_dps.get(ts.external_id) else None

                start = latest if latest else now - increment
                end = now
            
                dps_list = ice_cream_api.get_datapoints(timeseries_ext_id=ts.external_id, start=start, end=end)

                for dp_dict in dps_list:
                    dp_dict["instance_id"] = NodeId(space="icapi_dm_space", external_id=dp_dict["instance_id"])

                to_insert.extend(dps_list)

                if len(to_insert) > 50:
                    client.time_series.data.insert_multiple(datapoints=to_insert)
                    to_insert = []

            if to_insert:
                client.time_series.data.insert_multiple(datapoints=to_insert)
                print(f"  {hours}h of Datapoints took {default_timer() - big_start:.2f} seconds")
            else:
                print(f"  No TimeSeries, for {hours}h of Datapoints took {default_timer() - big_start:.2f} seconds")

        report_ext_pipe(client, "success")
    except Exception as e:
        report_ext_pipe(client, "fail", e)
