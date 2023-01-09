import os
import time
import shapefile
from shapely.geometry import mapping, shape
import shutil
import matplotlib.pyplot as plt

def relationship_maker_by_block_range_n_m(n,m, initial_block_number, final_block_number, borough_name):
    tic_master = time.perf_counter()
    boroughs = {"BX": "Bronx", "BK": "Brooklyn", "MN": "Manhattan", "QN": "Queens", "SI": "Staten_Island"}
    try:
        os.mkdir("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number))
    except FileNotFoundError as e:
        os.mkdir("{}{}".format(n,m))
        os.mkdir("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number))
    except FileExistsError as e:
        print("Directory already exists, deleting")
        shutil.rmtree("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number), ignore_errors=True)
        os.mkdir("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number))
    
    with (open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,n), 'a') as nodes_n_csv, 
            open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,m), 'a') as nodes_m_csv,
            open("{}{}/{}_{}_{}/edges.csv".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as edges_csv,
            open("{}{}/{}_{}_{}/neo4j_commands.txt".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as neo4j):
        
        edges_csv.write("Source,Target,Area,AreaA,AreaB\n")
    
    nodes_records_n = []
    nodes_records_m = []
    nodes_shapes_n = []
    nodes_shapes_m = []
    index = 0
    fields_n_types = {}
    fields_m_types = {}

    shp1 = "MapPLUTO_{}v2/{}/{}MapPLUTO.shp".format(n,boroughs.get(borough_name),borough_name)
    shp2 = "MapPLUTO_{}v2/{}/{}MapPLUTO.shp".format(m,boroughs.get(borough_name),borough_name)    
    if (int(n) >= 18):
        shp1 = "MapPLUTO_{}v2/MapPLUTO.shp".format(n,borough_name)
    if (int(m) >= 18):
        shp2 = "MapPLUTO_{}v2/MapPLUTO.shp".format(m,borough_name)
    with (shapefile.Reader(shp1) as shp_n, 
        shapefile.Reader(shp2) as shp_m, 
        open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,n), 'a') as nodes_n_csv, 
        open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,m), 'a') as nodes_m_csv,
        open("{}{}/{}_{}_{}/edges.csv".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as edges_csv,
        open("{}{}/{}_{}_{}/neo4j_commands.txt".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as neo4j):
        
        if(os.stat("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,n)).st_size == 0):
            fields_n = ['YearBBL:row.YearBBL','Year:row.Year']
            for field in [row[0] for row in shp_n.fields][1:]:
                fields_n.append("{}:row.{}".format(field,field))
            
            fields_m = ['YearBBL:row.YearBBL','Year:row.Year']
            for field in [row[0] for row in shp_m.fields][1:]:
                fields_m.append("{}:row.{}".format(field,field))
            
            nodes_n_csv.write("YearBBL," + ','.join([row[0] for row in shp_n.fields][1:]) + ",Year\n")
            neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{n}.csv\" AS uri\n")
            neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
            neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(n,','.join(fields_n)))
            neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{m}.csv\" AS uri\n")
            neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
            neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(m,','.join(fields_m)))
            neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/edges.csv\" AS uri\n")
            neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
            neo4j.write("MATCH (source:Lot20{} {{YearBBL: row.Source}})\n".format(n))
            neo4j.write("MATCH (target:Lot20{} {{YearBBL: row.Target}})\n".format(m))
            neo4j.write("""MERGE (source)-[:INTERSECTION {area: toFloat(row.Area),areaA: toFloat(row.AreaA),areaB: toFloat(row.AreaB)}]->(target)\n""")

            for record_row in shp_n.records():
                if record_row['Borough'] == borough_name:# and record_row['Block'] in range(initial_block_number,final_block_number+1):
                    nodes_records_n.append(record_row)
                    nodes_shapes_n.append(shp_n.shape(index))
                    index += 1    
            index = 0
            print(f"Carrega vetores, {len(nodes_records_n)} {len(nodes_shapes_n)}")
            for record_row in shp_m.records():
                if record_row['Borough'] == borough_name:#and record_row['Block'] in range(initial_block_number,final_block_number+1):
                    nodes_records_m.append(record_row)
                    nodes_shapes_m.append(shp_m.shape(index))
                    index +=1
            print(f"Carrega vetores aux, {len(nodes_records_m)} {len(nodes_shapes_m)}")

        if(os.stat("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,m)).st_size == 0):
            nodes_m_csv.write("YearBBL," + ','.join([row[0] for row in shp_m.fields][1:]) + ",Year\n")

        for block_number in range(initial_block_number,final_block_number+1):
            tic = time.perf_counter()
            
            index = 0
            nodes_names_list = []        
            for record_row in nodes_records_n:
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    forma_shapely = shape(nodes_shapes_n[index].__geo_interface__)
                    if "20{}{}".format(n,record_row['BBL']) not in nodes_names_list:
                        nodes_names_list.append("20{}{}".format(n,record_row['BBL']))
                        nodes_n_csv.write("20{}{},{},20{}\n".format(n,record_row['BBL'],line_formatter(record_row[0:]),n))
                        for idx, item in enumerate(record_row[0:]):
                            if idx not in fields_n_types:
                                if isinstance(item, int):
                                    fields_n_types[idx] = 'int'
                                if isinstance(item, float):
                                    fields_n_types[idx] = 'float'
                                if isinstance(item, str):
                                    fields_n_types[idx] = 'str'                                        
                        #nodes_n_csv.write("20{}{},{},{},{},20{}\n".format(n,record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address'],n))
                        #print("2010{},{},{},{},2010".format(record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address']))
                    index_aux = 0
                    for record_row_aux in nodes_records_m:
                        if record_row_aux['Block'] == block_number and record_row_aux['Borough'] == borough_name:
                            if "20{}{}".format(m,record_row_aux['BBL']) not in nodes_names_list:
                                nodes_names_list.append("20{}{}".format(m,record_row_aux['BBL']))
                                line_formatter(record_row_aux[0:])
                                nodes_m_csv.write("20{}{},{},20{}\n".format(m,record_row_aux['BBL'],line_formatter(record_row_aux[0:]),m))
                                for idx, item in enumerate(record_row_aux[0:]):
                                    if idx not in fields_m_types:
                                        if isinstance(item, int):
                                            fields_m_types[idx] = 'int'
                                        if isinstance(item, float):
                                            fields_m_types[idx] = 'float'
                                        if isinstance(item, str):
                                            fields_m_types[idx] = 'str'  
                                #nodes_m_csv.write("20{}{},{},{},{},20{}\n".format(m,record_row_aux['BBL'],record_row_aux['Borough'],record_row_aux['Block'],record_row_aux['Address'],m))
                                #print("2011{},{},{},{},2011".format(record_row_aux['BBL'],record_row_aux['Borough'],record_row_aux['Block'],record_row_aux['Address']))
                            forma_shapely_aux = shape(nodes_shapes_m[index_aux].__geo_interface__)
                            if(forma_shapely.intersects(forma_shapely_aux)):
                                intersect_area = forma_shapely.intersection(forma_shapely_aux).area
                                if intersect_area > 1:
                                    #registrando apenas intersecao com area maior q zero (estranho, mas o intersects ta dando true para intersecao com area zero)
                                    edges_csv.write("20{}{},20{}{},{},{},{}\n".format(n,record_row['BBL'],m,record_row_aux['BBL'],intersect_area,intersect_area/forma_shapely.area, intersect_area/forma_shapely_aux.area))
                                    if(intersect_area/forma_shapely.area < 0.98 or intersect_area/forma_shapely_aux.area < 0.98):
                                        print("Found intersection ratio lower than 0.98, verify block {}".format(block_number))
                        index_aux += 1
                index += 1 
            print(f"Borough {borough_name}, Block {block_number}, took {time.perf_counter()- tic:0.4f} seconds")
        
        fields_n = ['YearBBL:toInteger(row.YearBBL)','Year:toInteger(row.Year)']
        for idx,field in enumerate([row[0] for row in shp_n.fields][1:]):
            if fields_n_types[idx] == 'int': 
                fields_n.append("{}:toInteger(row.{})".format(field,field))
            elif fields_n_types[idx] == 'float': 
                fields_n.append("{}:toFloat(row.{})".format(field,field))
            elif fields_n_types[idx] == 'str': 
                fields_n.append("{}:row.{}".format(field,field))
                
        fields_m = ['YearBBL:toInteger(row.YearBBL)','Year:toInteger(row.Year)']
        for idx,field in enumerate([row[0] for row in shp_m.fields][1:]):
            if fields_m_types[idx] == 'int': 
                fields_m.append("{}:toInteger(row.{})".format(field,field))
            elif fields_m_types[idx] == 'float': 
                fields_m.append("{}:toFloat(row.{})".format(field,field))
            elif fields_m_types[idx] == 'str': 
                fields_m.append("{}:row.{}".format(field,field))
        neo4j.write(f'\n\n')
        neo4j.write(f'COMANDOS COM CONVERTERS\n')
        neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{n}.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(n,','.join(fields_n)))
        neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{m}.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(m,','.join(fields_m)))
        neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/edges.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("MATCH (source:Lot20{} {{YearBBL: toInteger(row.Source)}})\n".format(n))
        neo4j.write("MATCH (target:Lot20{} {{YearBBL: toInteger(row.Target)}})\n".format(m))
        neo4j.write("""MERGE (source)-[:INTERSECTION {area: toFloat(row.Area),areaA: toFloat(row.AreaA),areaB: toFloat(row.AreaB)}]->(target)\n""")
    print(f"Borough {borough_name}, Block {initial_block_number} to Block {final_block_number}, took {time.perf_counter()- tic_master:0.4f} seconds")

def line_formatter(line):
    resp = []
    for v in line:
       # if isinstance(v, int):
       #     v = float(v)
        if isinstance(v, str):
            v = v.replace(',',';')
        resp.append(str(v))
    return ','.join(resp).replace(',,',',NO DATA,').replace(',,',',NO DATA,')    
    #','.join(str(v).replace(',',';') for v in record_row_aux[0:]).replace(',,',',NO DATA,').replace(',,',',NO DATA,')

def read_by_bbl():
    shp1 = "MapPLUTO_18v2/MapPLUTO.shp"
    with (shapefile.Reader(shp1) as shp_n):
        print(shp_n.record(789720))
        for record_row in shp_n.records():
                index = 0
                if record_row['BBL'] == 1000010010:
                   print(record_row)
                   print(record_row.oid)
                   print(index)
                   print(list(shape(shp_n.shape(789720).__geo_interface__).exterior.coords))
                   plt.plot(*shape(shp_n.shape(789720).__geo_interface__).exterior.xy)
                   plt.show()
                index += 1   

def relationship_maker_by_block_range_n_m_record_oid(n,m, initial_block_number, final_block_number, borough_name):
    tic_master = time.perf_counter()
    boroughs = {"BX": "Bronx", "BK": "Brooklyn", "MN": "Manhattan", "QN": "Queens", "SI": "Staten_Island"}
    try:
        os.mkdir("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number))
    except FileNotFoundError as e:
        os.mkdir("{}{}".format(n,m))
        os.mkdir("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number))
    except FileExistsError as e:
        print("Directory already exists, deleting")
        shutil.rmtree("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number), ignore_errors=True)
        os.mkdir("{}{}/{}_{}_{}".format(n,m,borough_name,initial_block_number,final_block_number))
    
    with (open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,n), 'a') as nodes_n_csv, 
            open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,m), 'a') as nodes_m_csv,
            open("{}{}/{}_{}_{}/edges.csv".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as edges_csv,
            open("{}{}/{}_{}_{}/neo4j_commands.txt".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as neo4j):
        
        edges_csv.write("Source,Target,Area,AreaA,AreaB\n")
    
    nodes_records_n = []
    nodes_records_m = []
    nodes_shapes_n = []
    nodes_shapes_m = []
    index = 0
    fields_n_types = {}
    fields_m_types = {}

    shp1 = "MapPLUTO_{}v2/{}/{}MapPLUTO.shp".format(n,boroughs.get(borough_name),borough_name)
    shp2 = "MapPLUTO_{}v2/{}/{}MapPLUTO.shp".format(m,boroughs.get(borough_name),borough_name)    
    if (int(n) >= 18):
        shp1 = "MapPLUTO_{}v2/MapPLUTO.shp".format(n,borough_name)
    if (int(m) >= 18):
        shp2 = "MapPLUTO_{}v2/MapPLUTO.shp".format(m,borough_name)
    with (shapefile.Reader(shp1) as shp_n, 
        shapefile.Reader(shp2) as shp_m, 
        open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,n), 'a') as nodes_n_csv, 
        open("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,m), 'a') as nodes_m_csv,
        open("{}{}/{}_{}_{}/edges.csv".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as edges_csv,
        open("{}{}/{}_{}_{}/neo4j_commands.txt".format(n,m,borough_name,initial_block_number,final_block_number), 'a') as neo4j):
        
        if(os.stat("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,n)).st_size == 0):
            fields_n = ['YearBBL:row.YearBBL','Year:row.Year']
            for field in [row[0] for row in shp_n.fields][1:]:
                fields_n.append("{}:row.{}".format(field,field))
            
            fields_m = ['YearBBL:row.YearBBL','Year:row.Year']
            for field in [row[0] for row in shp_m.fields][1:]:
                fields_m.append("{}:row.{}".format(field,field))
            
            nodes_n_csv.write("YearBBL," + ','.join([row[0] for row in shp_n.fields][1:]) + ",Year\n")
            neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{n}.csv\" AS uri\n")
            neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
            neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(n,','.join(fields_n)))
            neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{m}.csv\" AS uri\n")
            neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
            neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(m,','.join(fields_m)))
            neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/edges.csv\" AS uri\n")
            neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
            neo4j.write("MATCH (source:Lot20{} {{YearBBL: row.Source}})\n".format(n))
            neo4j.write("MATCH (target:Lot20{} {{YearBBL: row.Target}})\n".format(m))
            neo4j.write("""MERGE (source)-[:INTERSECTION {area: toFloat(row.Area),areaA: toFloat(row.AreaA),areaB: toFloat(row.AreaB)}]->(target)\n""")

            for record_row in shp_n.records():
                if record_row['Borough'] == borough_name and record_row['Block'] in range(initial_block_number,final_block_number+1):
                    nodes_records_n.append(record_row)
                    nodes_shapes_n.append(shp_n.shape(record_row.oid))
                    index += 1    
            index = 0
            print(f"Carrega vetores, {len(nodes_records_n)} {len(nodes_shapes_n)}")
            for record_row in shp_m.records():
                if record_row['Borough'] == borough_name and record_row['Block'] in range(initial_block_number,final_block_number+1):
                    nodes_records_m.append(record_row)
                    nodes_shapes_m.append(shp_m.shape(record_row.oid))
                    index +=1
            print(f"Carrega vetores aux, {len(nodes_records_m)} {len(nodes_shapes_m)}")

        if(os.stat("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,m)).st_size == 0):
            nodes_m_csv.write("YearBBL," + ','.join([row[0] for row in shp_m.fields][1:]) + ",Year\n")

        for block_number in range(initial_block_number,final_block_number+1):
            tic = time.perf_counter()
            
            index = 0
            nodes_names_list = []        
            for record_row in nodes_records_n:
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    forma_shapely = shape(nodes_shapes_n[index].__geo_interface__)
                    if "20{}{}".format(n,record_row['BBL']) not in nodes_names_list:
                        nodes_names_list.append("20{}{}".format(n,record_row['BBL']))
                        nodes_n_csv.write("20{}{},{},20{}\n".format(n,record_row['BBL'],line_formatter(record_row[0:]),n))
                        for idx, item in enumerate(record_row[0:]):
                            if idx not in fields_n_types:
                                if isinstance(item, int):
                                    fields_n_types[idx] = 'int'
                                if isinstance(item, float):
                                    fields_n_types[idx] = 'float'
                                if isinstance(item, str):
                                    fields_n_types[idx] = 'str'                                        
                        #nodes_n_csv.write("20{}{},{},{},{},20{}\n".format(n,record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address'],n))
                        #print("2010{},{},{},{},2010".format(record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address']))
                    index_aux = 0
                    for record_row_aux in nodes_records_m:
                        if record_row_aux['Block'] == block_number and record_row_aux['Borough'] == borough_name:
                            if "20{}{}".format(m,record_row_aux['BBL']) not in nodes_names_list:
                                nodes_names_list.append("20{}{}".format(m,record_row_aux['BBL']))
                                line_formatter(record_row_aux[0:])
                                nodes_m_csv.write("20{}{},{},20{}\n".format(m,record_row_aux['BBL'],line_formatter(record_row_aux[0:]),m))
                                for idx, item in enumerate(record_row_aux[0:]):
                                    if idx not in fields_m_types:
                                        if isinstance(item, int):
                                            fields_m_types[idx] = 'int'
                                        if isinstance(item, float):
                                            fields_m_types[idx] = 'float'
                                        if isinstance(item, str):
                                            fields_m_types[idx] = 'str'  
                                #nodes_m_csv.write("20{}{},{},{},{},20{}\n".format(m,record_row_aux['BBL'],record_row_aux['Borough'],record_row_aux['Block'],record_row_aux['Address'],m))
                                #print("2011{},{},{},{},2011".format(record_row_aux['BBL'],record_row_aux['Borough'],record_row_aux['Block'],record_row_aux['Address']))
                            forma_shapely_aux = shape(nodes_shapes_m[index_aux].__geo_interface__)  

                            if(forma_shapely.intersects(forma_shapely_aux)):
                                intersect_area = forma_shapely.intersection(forma_shapely_aux).area
                                if intersect_area > 1:
                                    #registrando apenas intersecao com area maior q zero (estranho, mas o intersects ta dando true para intersecao com area zero)
                                    edges_csv.write("20{}{},20{}{},{},{},{}\n".format(n,record_row['BBL'],m,record_row_aux['BBL'],intersect_area,intersect_area/forma_shapely.area, intersect_area/forma_shapely_aux.area))
                                    #print("2010{},2011{},{},{},{}".format(record_row['BBL'],record_row_aux['BBL'],intersect_area,intersect_area/forma_shapely.area, intersect_area/forma_shapely_aux.area))
                                    if(intersect_area/forma_shapely.area < 0.98 or intersect_area/forma_shapely_aux.area < 0.98):
                                        print("Found intersection ratio lower than 0.98, verify block {}".format(block_number))
                        index_aux += 1
                index += 1 
            print(f"Borough {borough_name}, Block {block_number}, took {time.perf_counter()- tic:0.4f} seconds")
        
        fields_n = ['YearBBL:toInteger(row.YearBBL)','Year:toInteger(row.Year)']
        for idx,field in enumerate([row[0] for row in shp_n.fields][1:]):
            if fields_n_types[idx] == 'int': 
                fields_n.append("{}:toInteger(row.{})".format(field,field))
            elif fields_n_types[idx] == 'float': 
                fields_n.append("{}:toFloat(row.{})".format(field,field))
            elif fields_n_types[idx] == 'str': 
                fields_n.append("{}:row.{}".format(field,field))
                
        fields_m = ['YearBBL:toInteger(row.YearBBL)','Year:toInteger(row.Year)']
        for idx,field in enumerate([row[0] for row in shp_m.fields][1:]):
            if fields_m_types[idx] == 'int': 
                fields_m.append("{}:toInteger(row.{})".format(field,field))
            elif fields_m_types[idx] == 'float': 
                fields_m.append("{}:toFloat(row.{})".format(field,field))
            elif fields_m_types[idx] == 'str': 
                fields_m.append("{}:row.{}".format(field,field))
        neo4j.write(f'\n\n')
        neo4j.write(f'COMANDOS COM CONVERTERS\n')
        neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{n}.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(n,','.join(fields_n)))
        neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes20{m}.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("MERGE (c:Lot20{} {{{}}})\n\n".format(m,','.join(fields_m)))
        neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/edges.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("MATCH (source:Lot20{} {{YearBBL: toInteger(row.Source)}})\n".format(n))
        neo4j.write("MATCH (target:Lot20{} {{YearBBL: toInteger(row.Target)}})\n".format(m))
        neo4j.write("""MERGE (source)-[:INTERSECTION {area: toFloat(row.Area),areaA: toFloat(row.AreaA),areaB: toFloat(row.AreaB)}]->(target)\n""")
    print(f"Borough {borough_name}, Block {initial_block_number} to Block {final_block_number}, took {time.perf_counter()- tic_master:0.4f} seconds")
    
#relationship_maker_by_block_range_n_m('09', 10, 1, 3000, "MN")
#relationship_maker_by_block_range_n_m(10, 11, 1, 2500, "MN")
#relationship_maker_by_block_range_n_m(11, 12, 1, 2500, "MN")
#relationship_maker_by_block_range_n_m(12, 13, 1, 2500, "MN")
#relationship_maker_by_block_range_n_m(13, 14, 1, 2500, "MN")
#relationship_maker_by_block_range_n_m(14, 15, 1, 2500, "MN")
#relationship_maker_by_block_range_n_m(15, 16, 1, 2500, "MN")
#relationship_maker_by_block_range_n_m(16, 17, 1, 2500, "MN")
#relationship_maker_by_block_range_n_m(17, 18, 1, 2500, "MN")
#relationship_maker_by_block_range_n_m_record_oid(18, 19, 1, 2500, "MN")      
#relationship_maker_by_block_range_n_m_record_oid(19, 20, 1, 2500, "MN")  
#relationship_maker_by_block_range_n_m_record_oid(20, 21, 1, 2500, "MN")  

#relationship_maker_by_block_range_n_m('09', 10, 1, 2500, "BK")
#relationship_maker_by_block_range_n_m('09', 10, 1277, 2500, "BK")
#relationship_maker_by_block_range_n_m('09', 10, 1800, 2500, "BK")
#relationship_maker_by_block_range_n_m('09', 10, 1890, 2500, "BK") 3616 segundos
#relationship_maker_by_block_range_n_m('09', 10, 2500, 3000, "BK") 2554 segundos
#relationship_maker_by_block_range_n_m('09', 10, 3000, 3500, "BK") 3979 segundos
relationship_maker_by_block_range_n_m_record_oid('09',10, 3500, 4000, "BK") 
#relationship_maker_by_block_range_n_m(10, 11, 1, 2500, "BK")
#relationship_maker_by_block_range_n_m(11, 12, 1, 2500, "BK")
#relationship_maker_by_block_range_n_m(12, 13, 1, 2500, "BK")
#relationship_maker_by_block_range_n_m(13, 14, 1, 2500, "BK")
#relationship_maker_by_block_range_n_m(14, 15, 1, 2500, "BK")
#relationship_maker_by_block_range_n_m(15, 16, 1, 2500, "BK")
#relationship_maker_by_block_range_n_m(16, 17, 1, 2500, "BK")
#relationship_maker_by_block_range_n_m(17, 18, 1, 2500, "BK")
#relationship_maker_by_block_range_n_m_record_oid(18, 19, 1, 2500, "BK")      
#relationship_maker_by_block_range_n_m_record_oid(19, 20, 1, 2500, "BK")  
#relationship_maker_by_block_range_n_m_record_oid(20, 21, 1, 2500, "BK")      

'''                           try:
                                plt.xlabel("normal"+str(record_row['BBL']))
                                plt.plot(*forma_shapely.exterior.xy)
                                if(str(record_row['BBL']) == '1000010010.0' and str(record_row_aux['BBL']) == '1000010010.0' ):
                                    print("----")
                                    print("normal")
                                    print(str(record_row['BBL']))
                                    print(index)
                                    print(index_aux)
                                    print(list(forma_shapely.exterior.coords))
                                #plt.show()
                            except:
                                plt.xlabel("normal"+str(record_row['BBL']))
                                for geom in forma_shapely.geoms:    
                                    plt.plot(*geom.exterior.xy)
                                #plt.show()    
                            try:
                                plt.xlabel("aux"+str(record_row_aux['BBL']))
                                plt.plot(*forma_shapely_aux.exterior.xy)
                                if(str(record_row['BBL']) == '1000010010.0' and str(record_row_aux['BBL']) == '1000010010.0' ):
                                    print("----")
                                    print("aux")
                                    print(index)
                                    print(index_aux)
                                    print(str(record_row_aux['BBL']))
                                    print(list(forma_shapely_aux.exterior.coords))
                                #plt.show()
                            except:
                                plt.xlabel("aux"+str(record_row_aux['BBL']))
                                for geom in forma_shapely_aux.geoms:    
                                    plt.plot(*geom.exterior.xy)
                                #plt.show()  
'''