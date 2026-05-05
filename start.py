from utils.json_geojson_converter import JsonGeoJSON
from utils.scraping.paginating_download import PaginatingDownload
from utils.list_all_files import check_modified, list_files
from utils.list_all_files import create_folder_file
from utils.filter_search import FilterSearch
from utils.ui_checker import SelectChecker
import asyncio
import json

import re

import requests

import httpx

import geopandas as gpd
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


##### ENV FILE ######################
# kindly update your TOKEN in .env
auth_token = os.getenv("TOKEN", "")
api_base_url = os.getenv("API_BASE_URL", "https://example.com").rstrip("/")
url_project = os.getenv("URL_PROJ", f"{api_base_url}/v1/resources")
# change this because the pagination is disabled by default now
url_plot = os.getenv("URL_PLOT_FILTER", f"{api_base_url}/v1/resources/search")
url_activity_filter = os.getenv("URL_ACT_FILTER", f"{api_base_url}/v1/activities/search")
url_activity = os.getenv("URL_ACT_ID", f"{api_base_url}/v1/activities/")
url_patch_plot = os.getenv("URL_PATCH_PLOT", f"{api_base_url}/v1/resources/")
url_plot_details = os.getenv("URL_PLOT_DETAILS", f"{api_base_url}/v1/resources/details")

# variable ENV ## CHANGE THIS FOR LAND PLOTTING FILTER DOWNLOAD PROCESS
# change here or uncomment below (updating variable assignment to a filter value)
status = ''  # example in FSF
id = ''  # example in AXIS
plotLabels = ''

# plotName = 'Axis warrior' #uncomment this later because this one only for Axis Warrior

# additional filter for plot search - uncomment if necessary
# status='verified_tpi' # example in FSF
# id = 1244 #example in FSF
# plotLabels = "Go Zone"

#####################################


APP_BANNER = """
========================================
      Async API Downloader Utility
========================================
"""

print(APP_BANNER, "\n", "Welcome to the generic API downloader app \n --------------")

option_dict = {1: 'download', 2: 'update_plot'}

option_download = {1: 'download land plot and survey',
                   2: 'download tree survey'}

option_plot = {
    1: 'update land plotting'
}

uicheckerClass = SelectChecker(option_dict)
input_start = uicheckerClass.start_checker()
print("--------------- \n |",
      option_dict[input_start], " | selected \n---------------")


def get_proj_id(input_project_name):
    input_instance = FilterSearch(input_project_name, url_project, auth_token)
    searchedProj = input_instance.search_proj()
    searchedProj = input_instance.search_loop(searchedProj)
    print("search: ", searchedProj)
    return searchedProj


if input_start == 1:
    print("what type file that you want to download?")
    download_type = SelectChecker(option_download).start_checker()
    print("================= \n |",
          option_download[download_type], " | selected \n===================")

    if download_type == 1:
        input_project_name = input(
            "Please input a search keyword of Project or projectID number: or blank (enter directly) to list all project): \n")
        proj_list = get_proj_id(input_project_name)
        while len(proj_list) > 1:
            text_init = f"you have {len(proj_list)} list: {proj_list} \n please select one of them from above with the project ID -> \n \n"
            for i in proj_list:
                text_init += f"({i['id']}) for {i['newName']}, \n"
            print(text_init)

            input_project_name = input(
                "Type and Enter - Please input a single projectID number: ")
            proj_list = get_proj_id(input_project_name)
            continue
        print(
            f"you select: ({proj_list[0]['id']}) -> {proj_list[0]['newName']}")
        print(
            f"starting to download the land plot data({proj_list[0]['id']}) -> {proj_list[0]['newName']} \n-------------------------------------------------")
        print(f'Download all the status (recorded, verified_tpi etc) if you want to edit this, change in the argument classPagePlot instance in start.py from the variable env')

        # starting to process download input ID Project
        input_proj_id = proj_list[0]['id']
        input_proj_name = proj_list[0]['newName']
        # print(input_proj_id)

        async def downloading_json_plot():
            classPagePlot = PaginatingDownload(
                input_proj_id, url_plot, auth_token, status=status, id=id, plotLabels=plotLabels)
            # classPagePlot = PaginatingDownload(input_proj_id, url_plot, auth_token, status='verified_tpi') # example applying kwargs, additional api post payload
            total_pages_plot = await classPagePlot.total_pages()
            print(total_pages_plot)

            if total_pages_plot != 0 and total_pages_plot < 54:
                print('Checking is done, and total pages confirmed')
                print(f"we will do batch filter download for :")

                # downloading from the api data plot
                folder_json_api = './json_downloaded_api/plots'
                file_json_output = create_folder_file(
                    folder_json_api, str(input_proj_id), proj_list[0]['newName'])
                print(file_json_output, '\n----------------------------------')
                print('starting to write to json files')
                json_out = await classPagePlot.download_all_pages(
                    total_pages_plot, file_json_output)

            return [total_pages_plot, json_out, classPagePlot, file_json_output]

        print('Checking the total pages to verify the permission and process current page will be 0')

        async_list_id = asyncio.run(downloading_json_plot())

        total_pages_plot = async_list_id[0]
        json_out = async_list_id[1]
        classPagePlot = async_list_id[2]
        file_json_output = async_list_id[3]

        if total_pages_plot != 0 and total_pages_plot < 54:

            prop_data_list = json_out['rows']

            list_ids = [i['id'] for i in prop_data_list]
            print(list_ids)

            # constructor for list id
            a = list_ids
            b = ''

            for i in a:
                if i != a[len(a)-1]:
                    b += str(i) + ','
                else:
                    b += str(i)

            req_url_geojson = f'{url_plot_details}?ids={b}'

            print(f'requesting to: --> \n {req_url_geojson}')

            # converting to geojson from json
            folder_geojson_api = './00_GEOJSON_API/plots'
            file_geojson_output = create_folder_file(folder_geojson_api, str(
                input_proj_id), proj_list[0]['newName']+'_geojson')

            # commenting out because of the changing api in backend (no coordinate output)
            # jsonPlotClass = JsonGeoJSON(input_dict = json_out)
            # geojson_plot = jsonPlotClass.convert_plot_togeojson(file_geojson_output)  # variable return is json object

            # adjusting to the download geojson api instead, redownload with the list available in the downloaded json from first api (no geometry)
            async def downloading_v2():
                request_geojson_plot = PaginatingDownload(
                    input_proj_id, req_url_geojson, auth_token, request_type='downloadgeojsonplot', status=status, id=id, plotLabels=plotLabels)

                # downloading from the api data plot again!
                folder_json_api = './json_downloaded_api/plots'
                file_json_output_v2 = create_folder_file(
                    folder_json_api, str(input_proj_id), proj_list[0]['newName']+'_v2')

                print(file_json_output, '\n----------------------------------')
                print('starting to write to json files v2')
                json_out_v2 = await request_geojson_plot.dumping_json_geojson_get(
                    file_json_output_v2)

                return json_out_v2

            json_out_v2 = asyncio.run(downloading_v2())

            jsonPlotClass = JsonGeoJSON(input_dict=json_out_v2)
            geojson_plot = jsonPlotClass.convert_plot_togeojson(
                file_geojson_output)

            print(
                f'FILE DOWNLOADED AT {file_geojson_output} --------------------------------------------------------------------------- \n')
            # get the additional data from questionaire - land survey activity
            print('now we download land survey activity and join the data to geojson plot \n ---------------------------------------------')
            gdf_plot = gpd.GeoDataFrame.from_features(geojson_plot["features"])

            async def downloading_df(plot_id):
                print(f'downloading_json plot ID: {plot_id}')
                reqLandActClass = PaginatingDownload(input_proj_id, url_activity_filter, auth_token, projectId=input_proj_id,
                                                     request_type='get', activityType='land_survey', plotId=plot_id)
                request_get_landsurvey_id = await reqLandActClass.request_res()
                data_id = request_get_landsurvey_id.json()

                activity_id = data_id['rows'][0]['id']
                # print(activity_id)

                url_activity_id = f'{url_activity}{activity_id}'

                reqLandAct_ID_Class = PaginatingDownload(input_proj_id, url_activity_id, auth_token, projectId=input_proj_id,
                                                         request_type='get')

                request_get_landsurvey = await reqLandAct_ID_Class.request_res()
                data = request_get_landsurvey.json()

                # input_json_landsurvey = my_function(i).json()['rows']

                return data

            async def main_landsurvey():
                plotID_list = gdf_plot['plotID'].tolist()

                print('downloading_json for building geojson is started')
                # dfs_activity = []

                tasks = [downloading_df(plot_id) for plot_id in plotID_list]
                dfs_activity = await asyncio.gather(*tasks)

                return dfs_activity

            pre_con = 0

            dfs_activity = asyncio.run(main_landsurvey())

            pd_list = [pd.json_normalize(i) for i in dfs_activity]
            merged_df = pd.concat(pd_list, ignore_index=True)

            print(merged_df)

            folder_json_api = './json_downloaded_api/plots'
            file_json_output_async_id = create_folder_file(
                folder_json_api, str(input_proj_id), proj_list[0]['newName']+'_dfs_async')

            with open(file_json_output_async_id, 'w') as json_file:
                json.dump(dfs_activity, json_file)

            # Define the specific columns to include from the DataFrame
            columns_to_include = ['id', 'userID', 'plotID', 'startDate', 'endDate', 'synced', 'restarted',
                                        'note', 'mobileAppVersion', 'fullyCompleted', 'labels', 'comment',
                                        'commentAudio', 'measurementCount', 'totalSteps', 'preQuestionnaireID',
                                        'preQuestionnaireData', 'duplicateData', 'postQuestionnaireID',
                                        'postQuestionnaireData', 'deviceInformationID', 'status',
                                        'activityType', 'createdAt', 'outsidePolygon.crs.type',
                                        'outsidePolygon.crs.properties.name', 'outsidePolygon.type',
                                        'outsidePolygon.coordinates', 'activityTemplate.activityType',
                                        'activityTemplate.projectID', 'activityTemplate.id',
                                        'perfomedBy.firstName', 'perfomedBy.lastName', 'perfomedBy.id',
                                  ]

            # Perform the join using the specific columns
            merged_gdf = gdf_plot.merge(
                merged_df[columns_to_include], on='plotID', how='left')

            merged_gdf.rename(
                columns={"status_x": "status_plot"}, inplace=True)

            merged_gdf.rename(
                columns={"status_y": "status_survey"}, inplace=True)

            # print(merged_gdf.columns)

            file_geojson_output = create_folder_file(folder_geojson_api, str(
                input_proj_id), proj_list[0]['newName']+'_Joined_geojson')

            merged_geojson = jsonPlotClass.gpd_geojson(
                merged_gdf, file_geojson_output)
            pre_con = 1

            if pre_con == 0:
                input_download_vertices = 'n'
                print(input_download_vertices)
            else:
                ## vertices corner land survey #######################
                input_download_vertices = input('We will download vertices (corners) point location, please choose: \n \
                            type and enter: (y) yes to download corners points, or (n) no to quit the app (y/n): ')

            while input_download_vertices != 'y' or input_download_vertices != 'n':
                if input_download_vertices == 'y':
                    print(
                        'Now downloading the measurement vertices (corners) for the images (land eligibility check)')

                    with open(file_json_output_async_id, 'r') as json_file:
                        list_data = json.load(json_file)
                    print(list_data)

                    print(len(list_data))

                    pd_list = [pd.json_normalize(i) for i in list_data]

                    merged_df = pd.concat(pd_list, ignore_index=True)

                    list_activity_measurement = merged_df['measurement'].to_list()
                    print(list_activity_measurement)

                    list_measurement = []

                    for i in list_activity_measurement:
                        for x in i:
                            print(x)
                            print('------')
                            list_measurement.append(pd.json_normalize(x))

                    df_measurement = pd.concat(list_measurement, ignore_index=True)
                    print(df_measurement)

                    new_df = df_measurement
                    new_df[['latitude', 'longitude']] = new_df['gpsLocation'].str.split(
                        ',', expand=True).astype(float)

                    new_gdf = gpd.GeoDataFrame(new_df, geometry=gpd.points_from_xy(
                        new_df.longitude, new_df.latitude))

                    file_geojson_output = create_folder_file(folder_geojson_api, str(
                        input_proj_id), proj_list[0]['newName']+'_measurement_corner_geojson')
                    merged_geojson_mes_land = jsonPlotClass.gpd_geojson(
                        new_gdf, file_geojson_output)

                    break
                elif input_download_vertices == 'n':
                    print('Thank you please look the folder downloaded')
                    break
                else:
                    input_download_vertices = input('try again \n \
                            type and enter: (y) yes to download corners points, or (n) no to quit the app (y/n): ')

            ##############

            print(
                f'so far data json downloaded in projectID {str(input_proj_id)}: ')
            json_folder_plot = check_modified(
                f'{folder_json_api}/{str(input_proj_id)}/')
            geojson_folder_plot = check_modified(
                f'{folder_geojson_api}/{str(input_proj_id)}/')

        else:
            print(
                "you don't have permission to access the project ID - please ask the admin!")

    # if choose to download tree survey
    elif download_type == 2:
        print('will be updated later')
        '''
        input_project_name = input("Please input a search keyword of Project or projectID number: or blank (enter directly) to list all project): \n")
        proj_list = get_proj_id(input_project_name)
        while len(proj_list) > 1:
            text_init = f"you have {len(proj_list)} list: {proj_list} \n please select one of them from above with the project ID -> \n \n"
            for i in proj_list:
                text_init += f"({i['id']}) for {i['newName']}, \n"
            print(text_init)

            input_project_name = input("Type and Enter - Please input a single projectID number: ")
            proj_list = get_proj_id(input_project_name)
            continue
        print(f"you select: ({proj_list[0]['id']}) -> {proj_list[0]['newName']}")
        print(f"starting to download the land plot data({proj_list[0]['id']}) -> {proj_list[0]['newName']} \n-------------------------------------------------")
        print(f'Download all the status (recorded, verified_tpi etc) if you want to edit this, change in the argument classPagePlot instance in start.py from the variable env')

        # starting to process download input ID Project
        input_proj_id = proj_list[0]['id']
        input_proj_name = proj_list[0]['newName']
        #print(input_proj_id)

        # starting to download tree url_measurement based on projectid..
        print('Checking the total pages to verify the permission and process current page will be 0')
        classPagePlot = PaginatingDownload(input_proj_id, url_plot, auth_token, status=status, id=id, plotLabels=plotLabels)
        '''


elif input_start == 2:
    # print("what type file that you want to update? ")
    print("starting to update the polygon through API!")
    print('please choose your file in the ./01_update_polygon/00_input_to_update_and_backup/input/ folder')
    print('If you re not yet copy the file, please copy to: \n ./01_update_polygon/00_input_to_update_and_backup/input folder \n-----------------------------')

    list_data = list_files(
        './01_update_polygon/00_input_to_update_and_backup/input')

    shp_extension = '.shp'
    # other_extension = '.cpg'  # just add other extension and add also below if necessary

    json_extension = '.json'
    geojson_extension = '.geojson'

    list_geodata = [i for i in list_data if shp_extension in i
                    or json_extension in i
                    or geojson_extension in i
                    ]

    # print(list_geodata)

    num_select = uicheckerClass.input_update_shp(list_geodata)

    input_geodata = list_geodata[num_select-1]

    gdf_input = gpd.read_file(input_geodata)
    crs = gdf_input.crs
    crs_windows = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["Degree",0.0174532925199433],AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'

    if crs == 'EPSG:4326' or crs == crs_windows:
        print("verified - the input's crs is EPSG:4326")

        index_columns = gdf_input.columns
        list_columns = index_columns.tolist()

        a = 0
        print(
            f'Please choose which number is the plot id column of your input file {input_geodata}: ')

        num_select = uicheckerClass.input_update_shp(
            list_columns, add='to select which number that as PLOT ID Column')

        # print(num_select)

        act_input = num_select-1

        # Select the geometry series
        geometry_series = gdf_input['geometry']

        # Create an empty list to store the polygon dictionaries
        append_list_polygon = []

        # Iterate over each geometry and extract the coordinates
        for idx, geometry in enumerate(geometry_series):
            if geometry.geom_type == 'Polygon':
                # print('its polygon')
                # If it's a Polygon, extract the coordinates
                polygon_coordinates = [[coord[0], coord[1]]
                                       for coord in geometry.exterior.coords]
                append_list_polygon.append({'polygon': [polygon_coordinates]})
                
            else:
                print(
                    'the geometry is not polygon (probably multipolygon) please check your file first!')

        if len(append_list_polygon) > 0:

            list_plot = gdf_input[list_columns[act_input]].tolist()
            list_plot = [int(i) for i in list_plot]

            dict_plot = {list_plot[i]:append_list_polygon[i] for i in range(len(list_plot))}

            #for key,value in dict_plot.items():
            #    print(key, value)

            # constructor for list id/ plot
            a = list_plot
            b = ''

            for i in a:
                if i != a[len(a)-1]:
                    b += str(int(i)) + ','
                else:
                    b += str(int(i))

            req_url_geojson = f'{url_plot_details}?ids={b}'

            print(f'requesting to: --> \n {req_url_geojson}')

            async def request_backup(file_json_output):
                # backup first before update
                request_geojson_plot = PaginatingDownload(
                    '', req_url_geojson, auth_token, request_type='downloadgeojsonplot')
                
                # one id will be unique so that it only one page - will return as dictionary
                backup_plots = await request_geojson_plot.dumping_json_geojson_get(
                    file_json_output)
                
                return backup_plots


            text = input_geodata

            # Use regular expressions to find the last filename without extension
            match = re.search(r'\/([^/]+)\.\w+$', text)

            if match:
                filename_without_extension = match.group(1)
                print(filename_without_extension)
            else:
                print("No filename found")

            # downloading from the api data plot backup plot folder
            folder_json_api = './01_update_polygon/00_input_to_update_and_backup/backup'
            file_json_output = create_folder_file(
                folder_json_api, filename_without_extension, '_backup')

            backup_plot = asyncio.run(request_backup(file_json_output))

            # converting to geojson from json
            file_geojson_output = create_folder_file(folder_json_api, filename_without_extension, '_backup_geojson')
            jsonPlotClass = JsonGeoJSON(input_dict=backup_plot)
            geojson_plot = jsonPlotClass.convert_plot_togeojson(
            file_geojson_output)

            print(
                f'\n Backup is downloaded to {file_geojson_output} \n------------------------------')

            # updating process
            async def request_patch(plot_id, polygon_patch):
                class_patch = PaginatingDownload('', url_patch_plot, auth_token, 'patch_api', polygon_patch, plotId = plot_id)
                request_to_patch = await class_patch.request_res()
                
                print(f'patched {plot_id}')
                return request_to_patch
            
            async def main_request_patch():
                tasks = [request_patch(key, value) for key,value in dict_plot.items()]
                await asyncio.gather(*tasks)

            patching = asyncio.run(main_request_patch())
            print('patching polygon geometry is done')
            
            con = 0
            while con == 0:
                print('\n Now we want to re-download with requesting api and get each plot data from your edit - \n Remember that you can always do it later in the option 1 from the beginning \n please select yes (y) or no (n)')
                toDownload = input(
                    f"Do you want to download for total {len(list_plot)} plot and downloaded each plot? (y/n): ")
                if toDownload == 'y':
                    print('Now downloading the result file output: ')


                    # downloading from the api data plot for the result
                    folder_json_api = './01_update_polygon/01_output_updated'
                    file_json_output = create_folder_file(
                        folder_json_api, filename_without_extension, '_result')

                    result_plot = asyncio.run(request_backup(file_json_output))

                    # converting to geojson from json
                    file_geojson_output = create_folder_file(folder_json_api, filename_without_extension, '_result_geojson')
                    jsonPlotClass = JsonGeoJSON(input_dict=result_plot)
                    geojson_plot = jsonPlotClass.convert_plot_togeojson(
                    file_geojson_output)

                    print(
                        f'\n Result is downloaded to {file_geojson_output} \n------------------------------')

                    con = 1
                    break
                elif toDownload == 'n':
                    print(
                        'Thank you please download in batch filter in the option 1 from beginning if you want to download in batch')
                    break
                else:
                    toDownload = input('try again \n \
                            type and enter: (y) yes to download corners points, or (n) no to quit the app (y/n): ')

    else:
        print(f"sorry, the input data reference is not EPSG:4326 but {crs}")

print('\n--------------------------- END OF THE APP ------------------------------------------------')
