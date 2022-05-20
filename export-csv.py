import os
import time
import shapefile
from shapely.geometry import mapping, shape
import shutil

def relationship_maker_by_block_09_10(block_number, borough_name):
    tic = time.perf_counter()
    boroughs = {"BX": "Bronx", "BK": "Brooklyn", "MN": "Manhattan", "QN": "Queens", "SI": "Staten_Island"}
    
    try:
        os.mkdir("0910/{}_{}".format(borough_name,block_number))
    except FileNotFoundError as e:
        os.mkdir("0910")
        os.mkdir("0910/{}_{}".format(borough_name,block_number))
    except FileExistsError as e:
        print('teste')
        print("Directory already exists, going on")
   
    with (shapefile.Reader("MapPLUTO_09v2/{}/{}MapPLUTO.shp".format(boroughs.get(borough_name),borough_name)) as shp_2009, 
          shapefile.Reader("MapPLUTO_10v2/{}/{}MapPLUTO.shp".format(boroughs.get(borough_name),borough_name)) as shp_2010, 
            open("0910/{}_{}/nodes2009.csv".format(borough_name,block_number), 'w') as nodes2009_csv, 
            open("0910/{}_{}/nodes2010.csv".format(borough_name,block_number), 'w') as nodes2010_csv,
            open("0910/{}_{}/edges.csv".format(borough_name,block_number), 'w') as edges_csv,
            open("0910/{}_{}/neo4j_commands.txt".format(borough_name,block_number), 'w') as neo4j):
        
        nodes2009_csv.write("BBL,BOROUGH,BLOCK,ADDRESS,YEAR\n")
        nodes2010_csv.write("BBL,BOROUGH,BLOCK,ADDRESS,YEAR\n")
        edges_csv.write("Source,Target,Area,AreaA,AreaB\n")
        neo4j.write(f"WITH \"file:///0910/{borough_name}_{block_number}/nodes2009.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("""MERGE (c:Lot2009 {bbl:row.BBL,borough:row.BOROUGH,block:row.BLOCK,address:row.ADDRESS,year:row.YEAR})\n\n""")
        neo4j.write(f"WITH \"file:///0910/{borough_name}_{block_number}/nodes2010.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("""MERGE (c:Lot2010 {bbl:row.BBL,borough:row.BOROUGH,block:row.BLOCK,address:row.ADDRESS,year:row.YEAR})\n\n""")
        neo4j.write(f"WITH \"file:///0910/{borough_name}_{block_number}/edges.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("""MATCH (source:Lot2009 {bbl: row.Source})\n""")
        neo4j.write("""MATCH (target:Lot2010 {bbl: row.Target})\n""")
        neo4j.write("""MERGE (source)-[:INTERSECTION {area: toFloat(row.Area),areaA: toFloat(row.AreaA),areaB: toFloat(row.AreaB)}]->(target)\n""")
        
        nodes_records_2009 = []
        nodes_records_2010 = []
        nodes_shapes_2009 = []
        nodes_shapes_2010 = []
        index = 0
        for record_row in shp_2009.records():
            if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                nodes_records_2009.append(record_row)
                nodes_shapes_2009.append(shp_2009.shape(index))
            index += 1    
        index = 0
        for record_row in shp_2010.records():
            if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                nodes_records_2010.append(record_row)
                nodes_shapes_2010.append(shp_2010.shape(index))
            index +=1
        
        index = 0
        nodes_names_list = []        
        for record_row in nodes_records_2009:
            if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                forma_shapely = shape(nodes_shapes_2009[index].__geo_interface__)
                if "2009{}".format(record_row['BBL']) not in nodes_names_list:
                    nodes_names_list.append("2009{}".format(record_row['BBL']))
                    nodes2009_csv.write("2009{},{},{},{},2009\n".format(record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address']))
                    #print("2009{},{},{},{},2009".format(record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address']))
                index_aux = 0
                for record_row_aux in nodes_records_2010:
                    if record_row_aux['Block'] == block_number and record_row_aux['Borough'] == borough_name:
                        if "2010{}".format(record_row_aux['BBL']) not in nodes_names_list:
                            nodes_names_list.append("2010{}".format(record_row_aux['BBL']))
                            nodes2010_csv.write("2010{},{},{},{},2010\n".format(record_row_aux['BBL'],record_row_aux['Borough'],record_row_aux['Block'],record_row_aux['Address']))
                            #print("2010{},{},{},{},2010".format(record_row_aux['BBL'],record_row_aux['Borough'],record_row_aux['Block'],record_row_aux['Address']))
                        forma_shapely_aux = shape(nodes_shapes_2010[index_aux].__geo_interface__)
                        if(forma_shapely.intersects(forma_shapely_aux)):
                            intersect_area = forma_shapely.intersection(forma_shapely_aux).area
                            if intersect_area > 1:
                                #registrando apenas intersecao com area maior q zero (estranho, mas o intersects ta dando true para intersecao com area zero)
                                edges_csv.write("2009{},2010{},{},{},{}\n".format(record_row['BBL'],record_row_aux['BBL'],intersect_area,intersect_area/forma_shapely.area, intersect_area/forma_shapely_aux.area))
                                #print("2009{},2010{},{},{},{}".format(record_row['BBL'],record_row_aux['BBL'],intersect_area,intersect_area/forma_shapely.area, intersect_area/forma_shapely_aux.area))
                                if(intersect_area/forma_shapely.area < 0.98 or intersect_area/forma_shapely_aux.area < 0.98):
                                    print("Found intersection ratio lower than 0.98, verify block {}".format(block_number))
                    index_aux += 1
            index += 1 
    print(f"Borough {borough_name}, Block {block_number}, took {time.perf_counter()- tic:0.4f} seconds")    


def relationship_maker_by_block_range_09_10(initial_block_number, final_block_number, borough_name):
    tic_master = time.perf_counter()
    boroughs = {"BX": "Bronx", "BK": "Brooklyn", "MN": "Manhattan", "QN": "Queens", "SI": "Staten_Island"}
    try:
        os.mkdir("0910/{}_{}_{}".format(borough_name,initial_block_number,final_block_number))
    except FileNotFoundError as e:
        os.mkdir("0910")
        os.mkdir("0910/{}_{}_{}".format(borough_name,initial_block_number,final_block_number))
    except FileExistsError as e:
        print("Directory already exists, going on")
    
    with (open("0910/{}_{}_{}/nodes2009.csv".format(borough_name,initial_block_number,final_block_number), 'a') as nodes2009_csv, 
            open("0910/{}_{}_{}/nodes2010.csv".format(borough_name,initial_block_number,final_block_number), 'a') as nodes2010_csv,
            open("0910/{}_{}_{}/edges.csv".format(borough_name,initial_block_number,final_block_number), 'a') as edges_csv,
            open("0910/{}_{}_{}/neo4j_commands.txt".format(borough_name,initial_block_number,final_block_number), 'a') as neo4j):
        
        nodes2009_csv.write("BBL,BOROUGH,BLOCK,ADDRESS,YEAR\n")
        nodes2010_csv.write("BBL,BOROUGH,BLOCK,ADDRESS,YEAR\n")
        edges_csv.write("Source,Target,Area,AreaA,AreaB\n")
        neo4j.write(f"WITH \"file:///0910/{borough_name}_{initial_block_number}_{final_block_number}/nodes2009.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("""MERGE (c:Lot2009 {bbl:row.BBL,borough:row.BOROUGH,block:row.BLOCK,address:row.ADDRESS,year:row.YEAR})\n\n""")
        neo4j.write(f"WITH \"file:///0910/{borough_name}_{initial_block_number}_{final_block_number}/nodes2010.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("""MERGE (c:Lot2010 {bbl:row.BBL,borough:row.BOROUGH,block:row.BLOCK,address:row.ADDRESS,year:row.YEAR})\n\n""")
        neo4j.write(f"WITH \"file:///0910/{borough_name}_{initial_block_number}_{final_block_number}/edges.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("""MATCH (source:Lot2009 {bbl: row.Source})\n""")
        neo4j.write("""MATCH (target:Lot2010 {bbl: row.Target})\n""")
        neo4j.write("""MERGE (source)-[:INTERSECTION {area: toFloat(row.Area),areaA: toFloat(row.AreaA),areaB: toFloat(row.AreaB)}]->(target)\n""")

    for block_number in range(initial_block_number,final_block_number+1):
        tic = time.perf_counter()    
        with (shapefile.Reader("MapPLUTO_09v2/{}/{}MapPLUTO.shp".format(boroughs.get(borough_name),borough_name)) as shp_2009, 
            shapefile.Reader("MapPLUTO_10v2/{}/{}MapPLUTO.shp".format(boroughs.get(borough_name),borough_name)) as shp_2010, 
                open("0910/{}_{}_{}/nodes2009.csv".format(borough_name,initial_block_number,final_block_number), 'a') as nodes2009_csv, 
                open("0910/{}_{}_{}/nodes2010.csv".format(borough_name,initial_block_number,final_block_number), 'a') as nodes2010_csv,
                open("0910/{}_{}_{}/edges.csv".format(borough_name,initial_block_number,final_block_number), 'a') as edges_csv,
                open("0910/{}_{}_{}/neo4j_commands.txt".format(borough_name,initial_block_number,final_block_number), 'a') as neo4j):
            
            nodes_records_2009 = []
            nodes_records_2010 = []
            nodes_shapes_2009 = []
            nodes_shapes_2010 = []
            index = 0
            for record_row in shp_2009.records():
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    nodes_records_2009.append(record_row)
                    nodes_shapes_2009.append(shp_2009.shape(index))
                index += 1    
            index = 0
            for record_row in shp_2010.records():
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    nodes_records_2010.append(record_row)
                    nodes_shapes_2010.append(shp_2010.shape(index))
                index +=1
            
            index = 0
            nodes_names_list = []        
            for record_row in nodes_records_2009:
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    forma_shapely = shape(nodes_shapes_2009[index].__geo_interface__)
                    if "2009{}".format(record_row['BBL']) not in nodes_names_list:
                        nodes_names_list.append("2009{}".format(record_row['BBL']))
                        nodes2009_csv.write("2009{},{},{},{},2009\n".format(record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address']))
                        #print("2009{},{},{},{},2009".format(record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address']))
                    index_aux = 0
                    for record_row_aux in nodes_records_2010:
                        if record_row_aux['Block'] == block_number and record_row_aux['Borough'] == borough_name:
                            if "2010{}".format(record_row_aux['BBL']) not in nodes_names_list:
                                nodes_names_list.append("2010{}".format(record_row_aux['BBL']))
                                nodes2010_csv.write("2010{},{},{},{},2010\n".format(record_row_aux['BBL'],record_row_aux['Borough'],record_row_aux['Block'],record_row_aux['Address']))
                                #print("2010{},{},{},{},2010".format(record_row_aux['BBL'],record_row_aux['Borough'],record_row_aux['Block'],record_row_aux['Address']))
                            forma_shapely_aux = shape(nodes_shapes_2010[index_aux].__geo_interface__)
                            if(forma_shapely.intersects(forma_shapely_aux)):
                                intersect_area = forma_shapely.intersection(forma_shapely_aux).area
                                if intersect_area > 1:
                                    #registrando apenas intersecao com area maior q zero (estranho, mas o intersects ta dando true para intersecao com area zero)
                                    edges_csv.write("2009{},2010{},{},{},{}\n".format(record_row['BBL'],record_row_aux['BBL'],intersect_area,intersect_area/forma_shapely.area, intersect_area/forma_shapely_aux.area))
                                    #print("2009{},2010{},{},{},{}".format(record_row['BBL'],record_row_aux['BBL'],intersect_area,intersect_area/forma_shapely.area, intersect_area/forma_shapely_aux.area))
                                    if(intersect_area/forma_shapely.area < 0.98 or intersect_area/forma_shapely_aux.area < 0.98):
                                        print("Found intersection ratio lower than 0.98, verify block {}".format(block_number))
                        index_aux += 1
                index += 1 
        print(f"Borough {borough_name}, Block {block_number}, took {time.perf_counter()- tic:0.4f} seconds")    
    print(f"Borough {borough_name}, Block {initial_block_number} to Block {final_block_number}, took {time.perf_counter()- tic_master:0.4f} seconds")    

def relationship_maker_by_block_range_10_11(initial_block_number, final_block_number, borough_name):
    tic_master = time.perf_counter()
    boroughs = {"BX": "Bronx", "BK": "Brooklyn", "MN": "Manhattan", "QN": "Queens", "SI": "Staten_Island"}
    try:
        os.mkdir("1011/{}_{}_{}".format(borough_name,initial_block_number,final_block_number))
    except FileNotFoundError as e:
        os.mkdir("1011")
        os.mkdir("1011/{}_{}_{}".format(borough_name,initial_block_number,final_block_number))
    except FileExistsError as e:
        print("Directory already exists, going on")
    
    with (open("1011/{}_{}_{}/nodes2010.csv".format(borough_name,initial_block_number,final_block_number), 'a') as nodes2010_csv, 
            open("1011/{}_{}_{}/nodes2011.csv".format(borough_name,initial_block_number,final_block_number), 'a') as nodes2011_csv,
            open("1011/{}_{}_{}/edges.csv".format(borough_name,initial_block_number,final_block_number), 'a') as edges_csv,
            open("1011/{}_{}_{}/neo4j_commands.txt".format(borough_name,initial_block_number,final_block_number), 'a') as neo4j):
        
        nodes2010_csv.write("BBL,BOROUGH,BLOCK,ADDRESS,YEAR\n")
        nodes2011_csv.write("BBL,BOROUGH,BLOCK,ADDRESS,YEAR\n")
        edges_csv.write("Source,Target,Area,AreaA,AreaB\n")
        neo4j.write(f"WITH \"file:///1011/{borough_name}_{initial_block_number}_{final_block_number}/nodes2010.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("""MERGE (c:Lot2010 {bbl:row.BBL,borough:row.BOROUGH,block:row.BLOCK,address:row.ADDRESS,year:row.YEAR})\n\n""")
        neo4j.write(f"WITH \"file:///1011/{borough_name}_{initial_block_number}_{final_block_number}/nodes2011.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("""MERGE (c:Lot2011 {bbl:row.BBL,borough:row.BOROUGH,block:row.BLOCK,address:row.ADDRESS,year:row.YEAR})\n\n""")
        neo4j.write(f"WITH \"file:///1011/{borough_name}_{initial_block_number}_{final_block_number}/edges.csv\" AS uri\n")
        neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        neo4j.write("""MATCH (source:Lot2010 {bbl: row.Source})\n""")
        neo4j.write("""MATCH (target:Lot2011 {bbl: row.Target})\n""")
        neo4j.write("""MERGE (source)-[:INTERSECTION {area: toFloat(row.Area),areaA: toFloat(row.AreaA),areaB: toFloat(row.AreaB)}]->(target)\n""")

    for block_number in range(initial_block_number,final_block_number+1):
        tic = time.perf_counter()    
        with (shapefile.Reader("MapPLUTO_10v2/{}/{}MapPLUTO.shp".format(boroughs.get(borough_name),borough_name)) as shp_2010, 
            shapefile.Reader("MapPLUTO_11v2/{}/{}MapPLUTO.shp".format(boroughs.get(borough_name),borough_name)) as shp_2011, 
                open("1011/{}_{}_{}/nodes2010.csv".format(borough_name,initial_block_number,final_block_number), 'a') as nodes2010_csv, 
                open("1011/{}_{}_{}/nodes2011.csv".format(borough_name,initial_block_number,final_block_number), 'a') as nodes2011_csv,
                open("1011/{}_{}_{}/edges.csv".format(borough_name,initial_block_number,final_block_number), 'a') as edges_csv,
                open("1011/{}_{}_{}/neo4j_commands.txt".format(borough_name,initial_block_number,final_block_number), 'a') as neo4j):
            
            nodes_records_2010 = []
            nodes_records_2011 = []
            nodes_shapes_2010 = []
            nodes_shapes_2011 = []
            index = 0
            for record_row in shp_2010.records():
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    nodes_records_2010.append(record_row)
                    nodes_shapes_2010.append(shp_2010.shape(index))
                index += 1    
            index = 0
            for record_row in shp_2011.records():
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    nodes_records_2011.append(record_row)
                    nodes_shapes_2011.append(shp_2011.shape(index))
                index +=1
            
            index = 0
            nodes_names_list = []        
            for record_row in nodes_records_2010:
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    forma_shapely = shape(nodes_shapes_2010[index].__geo_interface__)
                    if "2010{}".format(record_row['BBL']) not in nodes_names_list:
                        nodes_names_list.append("2010{}".format(record_row['BBL']))
                        nodes2010_csv.write("2010{},{},{},{},2010\n".format(record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address']))
                        #print("2010{},{},{},{},2010".format(record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address']))
                    index_aux = 0
                    for record_row_aux in nodes_records_2011:
                        if record_row_aux['Block'] == block_number and record_row_aux['Borough'] == borough_name:
                            if "2011{}".format(record_row_aux['BBL']) not in nodes_names_list:
                                nodes_names_list.append("2011{}".format(record_row_aux['BBL']))
                                nodes2011_csv.write("2011{},{},{},{},2011\n".format(record_row_aux['BBL'],record_row_aux['Borough'],record_row_aux['Block'],record_row_aux['Address']))
                                #print("2011{},{},{},{},2011".format(record_row_aux['BBL'],record_row_aux['Borough'],record_row_aux['Block'],record_row_aux['Address']))
                            forma_shapely_aux = shape(nodes_shapes_2011[index_aux].__geo_interface__)
                            if(forma_shapely.intersects(forma_shapely_aux)):
                                intersect_area = forma_shapely.intersection(forma_shapely_aux).area
                                if intersect_area > 1:
                                    #registrando apenas intersecao com area maior q zero (estranho, mas o intersects ta dando true para intersecao com area zero)
                                    edges_csv.write("2010{},2011{},{},{},{}\n".format(record_row['BBL'],record_row_aux['BBL'],intersect_area,intersect_area/forma_shapely.area, intersect_area/forma_shapely_aux.area))
                                    #print("2010{},2011{},{},{},{}".format(record_row['BBL'],record_row_aux['BBL'],intersect_area,intersect_area/forma_shapely.area, intersect_area/forma_shapely_aux.area))
                                    if(intersect_area/forma_shapely.area < 0.98 or intersect_area/forma_shapely_aux.area < 0.98):
                                        print("Found intersection ratio lower than 0.98, verify block {}".format(block_number))
                        index_aux += 1
                index += 1 
        print(f"Borough {borough_name}, Block {block_number}, took {time.perf_counter()- tic:0.4f} seconds")    
    print(f"Borough {borough_name}, Block {initial_block_number} to Block {final_block_number}, took {time.perf_counter()- tic_master:0.4f} seconds")    

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
        
        #nodes_n_csv.write("BBL,BOROUGH,BLOCK,ADDRESS,YEAR\n")
        #nodes_m_csv.write("BBL,BOROUGH,BLOCK,ADDRESS,YEAR\n")
        edges_csv.write("Source,Target,Area,AreaA,AreaB\n")
        #neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes2010.csv\" AS uri\n")
        #neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        #neo4j.write("""MERGE (c:Lot2010 {bbl:row.BBL,borough:row.BOROUGH,block:row.BLOCK,address:row.ADDRESS,year:row.YEAR})\n\n""")
        #neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/nodes2011.csv\" AS uri\n")
        #neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        #neo4j.write("""MERGE (c:Lot2011 {bbl:row.BBL,borough:row.BOROUGH,block:row.BLOCK,address:row.ADDRESS,year:row.YEAR})\n\n""")
        #neo4j.write(f"WITH \"file:///{n}{m}/{borough_name}_{initial_block_number}_{final_block_number}/edges.csv\" AS uri\n")
        #neo4j.write("""LOAD CSV WITH HEADERS FROM uri AS row\n""")
        #neo4j.write("""MATCH (source:Lot2010 {bbl: row.Source})\n""")
        #neo4j.write("""MATCH (target:Lot2011 {bbl: row.Target})\n""")
        #neo4j.write("""MERGE (source)-[:INTERSECTION {area: toFloat(row.Area),areaA: toFloat(row.AreaA),areaB: toFloat(row.AreaB)}]->(target)\n""")
    
    nodes_records_n = []
    nodes_records_m = []
    nodes_shapes_n = []
    nodes_shapes_m = []
    index = 0
            
    for block_number in range(initial_block_number,final_block_number+1):
        tic = time.perf_counter()
        shp1 = "MapPLUTO_{}v2/{}/{}MapPLUTO.shp".format(n,boroughs.get(borough_name),borough_name)
        shp2 = "MapPLUTO_{}v2/{}/{}MapPLUTO.shp".format(m,boroughs.get(borough_name),borough_name)    
        if (n >= 18):
            shp1 = "MapPLUTO_{}v2/MapPLUTO.shp".format(n,borough_name)
        if (m >= 18):
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
                    if record_row['Borough'] == borough_name:
                        nodes_records_n.append(record_row)
                        nodes_shapes_n.append(shp_n.shape(index))
                        index += 1    
                index = 0
                print(f"Carrega vetores, {len(nodes_records_n)} {len(nodes_shapes_n)}")
                for record_row in shp_m.records():
                    if record_row['Borough'] == borough_name:
                        nodes_records_m.append(record_row)
                        nodes_shapes_m.append(shp_m.shape(index))
                        index +=1
                print(f"Carrega vetores aux,{len(nodes_records_m)} {len(nodes_shapes_m)}")

            if(os.stat("{}{}/{}_{}_{}/nodes20{}.csv".format(n,m,borough_name,initial_block_number,final_block_number,m)).st_size == 0):
                nodes_m_csv.write("YearBBL," + ','.join([row[0] for row in shp_m.fields][1:]) + ",Year\n")

            #aqui aquele comentario la de baixo
                      
            index = 0
            nodes_names_list = []        
            for record_row in nodes_records_n:
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    forma_shapely = shape(nodes_shapes_n[index].__geo_interface__)
                    if "20{}{}".format(n,record_row['BBL']) not in nodes_names_list:
                        nodes_names_list.append("20{}{}".format(n,record_row['BBL']))
                        nodes_n_csv.write("20{}{},{},20{}\n".format(n,record_row['BBL'],','.join(str(v).replace(',',';') for v in record_row[0:]).replace(',,',',NO DATA,').replace(',,',',NO DATA,'),n))
                        #nodes_n_csv.write("20{}{},{},{},{},20{}\n".format(n,record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address'],n))
                        #print("2010{},{},{},{},2010".format(record_row['BBL'],record_row['Borough'],record_row['Block'],record_row['Address']))
                    index_aux = 0
                    for record_row_aux in nodes_records_m:
                        if record_row_aux['Block'] == block_number and record_row_aux['Borough'] == borough_name:
                            if "20{}{}".format(m,record_row_aux['BBL']) not in nodes_names_list:
                                nodes_names_list.append("20{}{}".format(m,record_row_aux['BBL']))
                                nodes_m_csv.write("20{}{},{},20{}\n".format(m,record_row_aux['BBL'],','.join(str(v).replace(',',';') for v in record_row_aux[0:]).replace(',,',',NO DATA,').replace(',,',',NO DATA,'),m))
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
    print(f"Borough {borough_name}, Block {initial_block_number} to Block {final_block_number}, took {time.perf_counter()- tic_master:0.4f} seconds")    

#relationship_maker_by_block_09_10(100,"MN")
#relationship_maker_by_block_range_09_10(1,5000,"MN")    
#relationship_maker_by_block_range_10_11(1,5000,"MN")
#relationship_maker_by_block_range_n_m(11,12,1,5000,"MN")
#relationship_maker_by_block_range_n_m(12, 13, 1, 5000, "MN")
relationship_maker_by_block_range_n_m(13, 14, 193, 194, "MN")

"""         nodes_records_n = []
            nodes_records_m = []
            nodes_shapes_n = []
            nodes_shapes_m = []
            index = 0
            for record_row in shp_n.records():
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    nodes_records_n.append(record_row)
                    nodes_shapes_n.append(shp_n.shape(index))
                index += 1    
            index = 0
            for record_row in shp_m.records():
                if record_row['Block'] == block_number and record_row['Borough'] == borough_name:
                    nodes_records_m.append(record_row)
                    nodes_shapes_m.append(shp_m.shape(index))
                index +=1
"""           
