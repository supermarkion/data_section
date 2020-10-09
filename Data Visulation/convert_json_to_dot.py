import json

NULL_TYPE_CHECK = ", nullable"
DOT_FILE_TEMPLATE_PREFIX = "digraph A {\n" + \
                    "    node [ style = filled, fillcolor = white ];\n" + \
                    "    newrank = true;\n" + \
                    "    labelloc=\"t\";\n"


DOT_FILE_TEMPLATE_SUFFIX = "}"
INDENTATION = "    "
SUBGRAPH_TEXT = "subgraph cluster_"


def generate_preporcess_label(preprocessing_content):
    event_extract_flag = False
    label_detail = ""

    if preprocessing_content['type'] == 'conditional':
        label_detail = ",".join([e[0] + ':' + ";".join(str(v) for v in e[1]) if isinstance(e[1], list) else str(e[1]) for e in preprocessing_content['definition'].items()])
    if preprocessing_content['type'] == 'elapsed_time':
        label_detail = ",".join([str(e[0]) + ':' + str(e[1]) for e in preprocessing_content['definition'].items()])
    if preprocessing_content['type'] == 'query':
        label_detail = preprocessing_content['query']
    if preprocessing_content['type'] == 'event_extraction':
        event_extract_flag = True
        label_detail = preprocessing_content['predicted'] + ":" + preprocessing_content['truth'] if 'truth' in preprocessing_content.keys() else "" + ", Event level tableau format"
        event_extract_flag = True
    
    # replace the "" at end of day
    label_detail = label_detail.replace("\"", "\'")

    return label_detail, event_extract_flag


def generate_preprocesses_content(preprocess_input_content, step_name, source_prefix):
    # source prefix can be: merge_result_ or data_validation_result
    preprocessings_output_content = ""
    last_node_name = ""
    last_label_detail = ""
    event_extract_flag = False

    # it is a water fall model, and index is important
    for i in range(len(preprocess_input_content.keys())):
        preprocessing_name = list(preprocess_input_content.keys())[i]
        preprocessing_content = preprocess_input_content[preprocessing_name]

        pre_label_detail = ""
        current_label_detail = ""

        if i != 0:
            pre_preprocessing_content = preprocess_input_content[list(preprocess_input_content.keys())[i -1 ]]
            pre_label_detail, _ = generate_preporcess_label(pre_preprocessing_content)

        current_label_detail, event_extract_flag = generate_preporcess_label(preprocessing_content)

        # insert the current node    
        preprocessings_output_content = preprocessings_output_content + INDENTATION*2 + preprocessing_name + "_" + step_name + "[shape=cds, label=\"" + preprocessing_content['type'] + "\"]\n"

        # if this is the first node, we should link it form data loader merge result: #merge_result_
        if i == 0:
            if source_prefix != '':
                preprocessings_output_content = preprocessings_output_content + INDENTATION*2 + source_prefix + step_name + " -> " + preprocessing_name + "_" + step_name + ";\n"
        else:
            pre_preprocessing_name = list(preprocess_input_content.keys())[i - 1]

            # link from pre node to current one            
            if pre_label_detail and pre_label_detail != '':
                preprocessings_output_content = preprocessings_output_content + INDENTATION*2 + pre_preprocessing_name + "_" + step_name + " -> " + preprocessing_name + "_" + step_name + "[label=\"" + pre_label_detail + "\"];\n"
            else:
                preprocessings_output_content = preprocessings_output_content + INDENTATION*2 + pre_preprocessing_name + "_" + step_name + " -> " + preprocessing_name + "_" + step_name + ";\n"

        last_node_name = preprocessing_name
        last_label_detail = current_label_detail if current_label_detail and current_label_detail != '' else ""
                    
    return preprocessings_output_content, last_node_name, last_label_detail, event_extract_flag

def extract_relative_path(dataset_file_full_path):
    relative_path = dataset_file_full_path.split('training')[-1].split('validation')[-1].split('DATA_WIP')[-1]
    if relative_path.startswith("/"):
        return relative_path[1:]
    else:
        return relative_path

def generate_measurment_content(dataset_loader_content, dataset_name):
    measurment_content = ""
    # prepare measurment input files
    if 'path' in dataset_loader_content.keys():
        #print(dataset_loader_content['path'])
        measurment_content = measurment_content + INDENTATION*2 + SUBGRAPH_TEXT + "measurment_" + dataset_name + " {\n"
        measurment_content = measurment_content + INDENTATION*3 + "label = \"measurment_files\";\n"
        measurment_content = measurment_content + INDENTATION*3 + "input_measurment_files_" + dataset_name + " [shape=folder, label=\"" + \
            dataset_loader_content['path'].replace('\\', '/').split('/')[-1] + '/*' + dataset_loader_content['type'] + "\"];\n"
        measurment_content = measurment_content + INDENTATION*2 + "}\n"
        return measurment_content
    else:
        raise KeyError('No path section in REPORT data loader part.')

def generate_truth_content(dataset_loader_content, dataset_name):
    truth_content = ""
    # prepare truth input files
    # I assume at lease we should have one of them: 1) metadata path, 2) truth, 3) query

    truth_content = truth_content + INDENTATION*2 + SUBGRAPH_TEXT + "cluster_truth_files" + dataset_name + " {\n"
    truth_content = truth_content + INDENTATION*3 + "label = \"truth_files\";\n"

    if 'metadata' in dataset_loader_content.keys() and 'metadatapath' in dataset_loader_content['metadata'].keys():
        metadata_path_name = extract_relative_path(dataset_loader_content['metadata']['metadatapath'])
        truth_content = truth_content + INDENTATION*3 + "metadata_file_" + dataset_name + " [shape=folder, label=\"" + metadata_path_name + "\"];\n"

    if 'metadatapath' in dataset_loader_content.keys():
        metadata_path_name = extract_relative_path(dataset_loader_content['metadatapath'])
        truth_content = truth_content + INDENTATION*3 + "metadata_file_" + dataset_name + " [shape=folder, label=\"" + metadata_path_name + "\"];\n"

    if 'query' in dataset_loader_content.keys():
        query_path_name = extract_relative_path(dataset_loader_content['query'])
        truth_content = truth_content + INDENTATION*3 + "query_file_" + dataset_name + " [shape=folder, label=\"" + query_path_name + "\"];\n"

    if 'truth' in dataset_loader_content.keys() and dataset_loader_content['truth']['path']:
        truth_path_name = extract_relative_path(dataset_loader_content['truth']['path'])
        truth_content = truth_content + INDENTATION*3 + "truth_files_" + dataset_name + " [shape=folder, label=\"" + truth_path_name + "\"];\n"

    # force generate a placeholder node to make sure following preporcess logic easy to handle

    truth_content = truth_content + INDENTATION*2 + "}\n"

    return truth_content


def generate_dataset_loader(dataset_loader_content, dataset_name):

    truth_merge_flag = False
    metadata_merge_flag = False
    query_merge_flag = False

    dataset_loader_content_result = ""

    # generate dataset data loader graph title
    dataset_data_loader_title = INDENTATION * 1 + SUBGRAPH_TEXT + dataset_name
    dataset_loader_content_result = dataset_loader_content_result + dataset_data_loader_title + " {\n"

    dataset_data_loader_label = INDENTATION * 2 + "label = <<b>data_loader_" + dataset_name + "</b>>;\n"

    dataset_loader_content_result = dataset_loader_content_result + dataset_data_loader_label
    #print(dataset_loader_content)
    measurment_content = generate_measurment_content(dataset_loader_content, dataset_name)

    if measurment_content and measurment_content != '':
        dataset_loader_content_result = dataset_loader_content_result + measurment_content

    truth_content = generate_truth_content(dataset_loader_content, dataset_name)
    if truth_content and truth_content != '':
        dataset_loader_content_result = dataset_loader_content_result + truth_content

    # of course, once we load measurement and truth, then we merge them together
    if measurment_content and truth_content and measurment_content != '' and truth_content != '':
        # if we has truth file, then we should use frame joinon
        if 'truth' in dataset_loader_content.keys() and dataset_loader_content['truth']['path']: # I am wonder should I assigh these flag values to the current class
            # add joinon node
            dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "truth_joinon_" + dataset_name + " [shape=invhouse, label=\"" + '=='.join(dataset_loader_content['truth']['joinon']) + "\"];\n"
            # link measurment files and truth files to joinon node
            dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "input_measurment_files_" + dataset_name + " -> truth_joinon_" + dataset_name + ";\n"
            dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "truth_files_" + dataset_name + " -> truth_joinon_" + dataset_name + ";\n"

            truth_merge_flag = True

        # if we has metadata file, then we should use ShortFilename joinon
        if ('metadata' in dataset_loader_content.keys() and 'metadatapath' in dataset_loader_content['metadata'].keys()) or 'metadatapath' in dataset_loader_content.keys():
            if 'metadata_filename_col' in dataset_loader_content.keys():
                join_logic = '=='.join([dataset_loader_content['metadata_filename_col'], "ShortFilename"])
            else:
                join_logic = '=='.join(["gt|eod|file_path", "ShortFilename"])
            # add joinon node

            dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "metadata_joinon_" + dataset_name + " [shape=invhouse, label=\"" + join_logic + "\"];\n"
            
            if truth_merge_flag: # if we already do the truth merge, should come from truth merge
                dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "truth_joinon_" + dataset_name + " -> metadata_joinon_" + dataset_name + ";\n"
            else:
                dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "input_measurment_files_" + dataset_name + " -> metadata_joinon_" + dataset_name + ";\n"
            dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "metadata_file_" + dataset_name + " -> metadata_joinon_" + dataset_name + ";\n"

            metadata_merge_flag = True

        # if we has query file, then we should use ShortFilename filter it
        if 'query' in dataset_loader_content.keys():
            dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "query_joinon_" + dataset_name + " [shape=invhouse, label=\"keep ShortFilename in query file\"];\n"
            
            if metadata_merge_flag: # if we already do the metadata merge, should come from metadata merge
                dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "metadata_joinon_" + dataset_name + " -> query_joinon_" + dataset_name + ";\n"
            else:
                dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "input_measurment_files_" + dataset_name + " -> query_joinon_" + dataset_name + ";\n"
            dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "query_file_" + dataset_name + " -> query_joinon_" + dataset_name + ";\n"

            query_merge_flag = True

    dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "merge_result_" + dataset_name + " [shape=cylinder, label=\"frame level tableau format\"];\n"

    if query_merge_flag:
        dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "query_joinon_" + dataset_name + " -> merge_result_" + dataset_name + ";\n"
    elif metadata_merge_flag:
        dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "metadata_joinon_" + dataset_name + " -> merge_result_" + dataset_name + ";\n"
    elif truth_merge_flag:
        dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "truth_joinon_" + dataset_name + " -> merge_result_" + dataset_name + ";\n"
    else: # or we link from measurment file directly
        dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "input_measurment_files_" + dataset_name + " -> merge_result_" + dataset_name + ";\n"

    last_node_name = ""
    last_label_detail = ""
    event_extract_flag = False

    # prepare the preprocessing section
    if 'preprocessing' in dataset_loader_content.keys():
        preprocessings_output_content, last_node_name, last_label_detail, event_extract_flag = generate_preprocesses_content(dataset_loader_content['preprocessing'], dataset_name, "merge_result_")
        dataset_loader_content_result = dataset_loader_content_result + preprocessings_output_content
    else: # if no preporess step, we link merge_result_ to data_loader_result result directly
        dataset_loader_content_result = dataset_loader_content_result + INDENTATION*2 + "merge_result_" + dataset_name + " -> data_loader_result" + ";\n"

    dataset_loader_content_result = dataset_loader_content_result + INDENTATION*1 + "}\n"

    if last_node_name and last_node_name != "":

        if last_label_detail and last_label_detail != "":
            dataset_loader_content_result = dataset_loader_content_result + INDENTATION*1 + last_node_name + "_" + dataset_name + " -> data_loader_result [label=\"" + last_label_detail + "\"];\n"
        else:
            dataset_loader_content_result = dataset_loader_content_result + INDENTATION*1 + last_node_name + "_" + dataset_name + " -> data_loader_result;\n"
            
    return dataset_loader_content_result, event_extract_flag


def generate_data_loader_stage(input_json_content):

    datasets_loader_content = ""

    event_extract_flag = False

    if 'data_loader' in list(input_json_content.keys()):

        # it should has cwd and data, and we ignore the cwd
        if 'data' in list(input_json_content['data_loader'].keys()):
            datasets_loader = input_json_content['data_loader']['data']
            for dataset_name in datasets_loader.keys():
                dataset_loader_content, event_extract_flag = generate_dataset_loader(datasets_loader[dataset_name], dataset_name)
                datasets_loader_content = datasets_loader_content + dataset_loader_content

            # insert the final data loader result
            if event_extract_flag:
                datasets_loader_content = datasets_loader_content + INDENTATION*1 + "data_loader_result [shape=cylinder, label=\"Event level tableau data\"]\n"
            else:
                datasets_loader_content = datasets_loader_content + INDENTATION*1 + "data_loader_result [shape=cylinder, label=\"Frame level tableau data\"]\n"

            return datasets_loader_content, event_extract_flag

        else:
            raise KeyError('No data section in REPORT data loader part.')

def generate_data_validaton_stage(input_json_content, event_extract_flag):
    datasets_validation_content = ""
    last_v_type_step_name = ""

    if "data_validation" in input_json_content.keys():

        # sometimes, the value type is wired :(, so we clean the data validation steps in case the index error
        removed_key_list = []
        for validation_step in input_json_content['data_validation']:
            if not isinstance(input_json_content['data_validation'][validation_step], dict):
                removed_key_list.append(validation_step)
        for removed_key in removed_key_list:       
            del input_json_content['data_validation'][removed_key]

        if len(input_json_content['data_validation']) > 0:
            datasets_validation_content = datasets_validation_content + INDENTATION*2  + "data_loader_result -> " + "v_type_0" + ";\n"

        datasets_validation_content = datasets_validation_content + INDENTATION*1 + SUBGRAPH_TEXT + "data_validation {\n"
        datasets_validation_content = datasets_validation_content + INDENTATION*2 + "label = <<b>data_validation</b>>;\n"

        for i in range(len(input_json_content['data_validation'].keys())):
            column_name = list(input_json_content['data_validation'].keys())[i]
            validation_label = column_name + ': ' + input_json_content['data_validation'][column_name]['dtype']
            if 'nullable' in input_json_content['data_validation'][column_name].keys():
                validation_label = validation_label + NULL_TYPE_CHECK
            
            validation_step_name = "v_type_" + str(i)

            # insert current node
            datasets_validation_content = datasets_validation_content + INDENTATION*2 + validation_step_name + " [shape=cds, label=\"" + validation_label + "\"];\n" 
            
            if i != 0: # link from data validation result to point current node
                datasets_validation_content = datasets_validation_content + INDENTATION*2  + "v_type_" + str(i - 1) + " -> " + validation_step_name + ";\n"

            if i == len(input_json_content['data_validation'].keys()) - 1:
                last_v_type_step_name = validation_step_name


        datasets_validation_content = datasets_validation_content + INDENTATION*1 + "}\n"

        # insert the final data validation result
        if event_extract_flag:
            datasets_validation_content = datasets_validation_content + INDENTATION*1 + "data_validation_result [shape=cylinder, label=\"Event level tableau data\"]\n"
        else:
            datasets_validation_content = datasets_validation_content + INDENTATION*1 + "data_validation_result [shape=cylinder, label=\"Frame level tableau data\"]\n"
        
        datasets_validation_content = datasets_validation_content + INDENTATION*1  + last_v_type_step_name + " -> data_validation_result;\n"

        return datasets_validation_content

    else:
        raise KeyError('No Data Validation section: data_validation in REPORT data loader part.')


def generate_metric_task(generic_metric_task_content, task_name):
    task_content = ""

    # if no preprocess within task, we can link from data validation result to here directly.
    if 'preprocessing' in generic_metric_task_content:
        # only link to first node
        metric_task_preprocessing_step_1 = list(generic_metric_task_content['preprocessing'].keys())[0] + "_" + task_name
        task_content = task_content + INDENTATION*1 + "data_validation_result -> " + metric_task_preprocessing_step_1 + " ;\n"

    else: # if no preprocess, link to result directly
        task_content = task_content + INDENTATION*1 + "data_validation_result -> metric_and_result_" + task_name + " ;\n"

     # insert the task graph information
    task_content = task_content + INDENTATION*1 + SUBGRAPH_TEXT + "metric_and_result_" + task_name + " {\n"

    if 'title' in generic_metric_task_content.keys():
        task_content = task_content + INDENTATION*2 + "label = <<b>" + generic_metric_task_content['title'].replace('<', '').replace('>', '').replace('(', '').replace(')', '').replace('&', '').replace('{', '').replace('}', '') + "</b>>;\n"
    elif 'cwd' in generic_metric_task_content.keys():
        task_content = task_content + INDENTATION*2 + "label = <<b>" + generic_metric_task_content['cwd'].replace('<', '').replace('>', '').replace('(', '').replace(')', '').replace('&', '').replace('{', '').replace('}', '') + "</b>>;\n"

    if 'preprocessing' in generic_metric_task_content:
        preprocessings_output_content, last_node_name, last_label_detail, _ = generate_preprocesses_content(generic_metric_task_content['preprocessing'], task_name, "")
        
        if last_node_name and last_node_name != "":

            if last_label_detail and last_label_detail != "":
                preprocessings_output_content = preprocessings_output_content + INDENTATION*2 + last_node_name + "_" + task_name + " -> metric_and_result_" + task_name + "[label=\"" + last_label_detail + "\"];\n"
            else:
                preprocessings_output_content = preprocessings_output_content + INDENTATION*2 + last_node_name + "_" + task_name + " -> metric_and_result_" + task_name + ";\n"
                
        task_content = task_content + preprocessings_output_content

    plot_display = generic_metric_task_content['plot'] if 'plot' in generic_metric_task_content.keys() else ""

    if plot_display == "": 
        plot_display = generic_metric_task_content['type'] if 'type' in generic_metric_task_content.keys() else ""

    if 'x_label' in generic_metric_task_content.keys():
        plot_display = plot_display + '|x_label:' + generic_metric_task_content['x_label'] # no way to has more than one x_label I guess
    if 'y_label' in generic_metric_task_content.keys():
        plot_display = plot_display + '|y_label:' + str(generic_metric_task_content['y_label']) if not isinstance(generic_metric_task_content['y_label'], list) else ';'.join([str(e) for e in generic_metric_task_content['y_label']])

    if 'truth' in generic_metric_task_content.keys():
        plot_display = plot_display + '|truth:' + generic_metric_task_content['truth']
    if 'predicted' in generic_metric_task_content.keys():

        if isinstance(generic_metric_task_content['predicted'], list):
            plot_display = plot_display + '|predicted:' + ';'.join(generic_metric_task_content['predicted'])
        else:
            plot_display = plot_display + '|predicted:' + generic_metric_task_content['predicted']

    task_content = task_content + INDENTATION*2 + "metric_and_result_" + task_name + "[shape=record,style=note,label=\"{" + plot_display + "}\"];\n"

    task_content = task_content + INDENTATION*1 + "}\n"

    return task_content


def generate_metric_and_result(generic_metric_input_content):
    
    metric_and_result_content = ""

    for task_name in generic_metric_input_content.keys():
        task_content = generate_metric_task(generic_metric_input_content[task_name], task_name)
        metric_and_result_content = metric_and_result_content + task_content

    return metric_and_result_content

def generate_metric_and_result_stage(input_json_content):

    if 'plugin_configs' in input_json_content.keys() and 'generic_metric' in input_json_content['plugin_configs']:
        generic_metric_task_content = input_json_content['plugin_configs']['generic_metric']

        if 'task' in generic_metric_task_content.keys() and 'tasks' in generic_metric_task_content['task']:
            return generate_metric_and_result(generic_metric_task_content['task']['tasks'])
        else:
            print('No Metric and Result section: task or tasks in REPORT data loader part.')
            return ""
    else:
        print('No Metric and Result section: plugin_configs or generic_metric in REPORT data loader part.')
        return ""


def pick_perf_report_title(input_json_content):
    if 'artifact_folder' in list(input_json_content.keys()):
        return input_json_content['artifact_folder'].replace('\\', '/').split('REPORT/')[-1].replace('/', '-')


def generate_dot_content(input_json_content):
    DOT_FILE_BODY = ""

    # pick up title
    perf_title = pick_perf_report_title(input_json_content)

    if perf_title and perf_title != '':
        DOT_FILE_BODY = DOT_FILE_BODY + INDENTATION*1 + "label=\""+ perf_title + "\"\n"

    # generate data loader
    perf_data_loader, event_extract_flag = generate_data_loader_stage(input_json_content)

    if perf_data_loader and perf_data_loader != '':
        DOT_FILE_BODY = DOT_FILE_BODY + perf_data_loader

    # generate data validation
    perf_data_validation = generate_data_validaton_stage(input_json_content, event_extract_flag)
    if perf_data_validation and perf_data_validation != '':
        DOT_FILE_BODY = DOT_FILE_BODY + perf_data_validation

    # generate metric and result
    perf_metric_and_result = generate_metric_and_result_stage(input_json_content)
    if perf_metric_and_result and perf_metric_and_result != '':
        DOT_FILE_BODY = DOT_FILE_BODY + perf_metric_and_result

    ouptut_dot_file_content = DOT_FILE_TEMPLATE_PREFIX + DOT_FILE_BODY + DOT_FILE_TEMPLATE_SUFFIX
    
    return ouptut_dot_file_content


def return_dot_file(input_json_file):
    with open(input_json_file, 'r') as json_file:
        input_json_content = json.load(json_file)
        ouptut_dot_file_content = generate_dot_content(input_json_content=input_json_content)
        return ouptut_dot_file_content

