import sqlparse
import sys
import re
import numpy as np
import operator
import string

and_flag = 0
or_flag = 0
sum_flag = 0
max_flag = 0
min_flag = 0
avg_flag = 0
distinct_flag = 0
where_flag = 0
begin_table = 0

pplist = []
left_cond = []
right_cond = []
columns = []
tables = []
whereConds = []
meta_table = []
meta_cols = []
my_cols2 = [] 
my_cols1 = []
oper_dict = {'+': lambda a,b: a+b, 
            '-': lambda a,b: a-b, 
            '>': lambda a,b: a>b, 
            '<': lambda a,b: a<b, 
            '=': lambda a,b:a==b,
            '<=':lambda a,b:a<=b,
            '>=':lambda a,b:a>=b}


def pprint(x):
    if not hasattr(x[0], '__iter__'):
        x = [x]
    print '\n'.join(['\t'.join(map(str, p)) for p in x])
    #pplist.append('\n'.join(["".join(map(str, p)) for p in x]))
    #print "pp:", pplist

def call_function(cond):
    global my_cols1
    global my_cols2
    table_name = tables[0] + ".csv"
    #print table_name
    my_csv = np.genfromtxt(table_name, delimiter=',')
    #print "c1:",my_cols1
    y = my_cols1.index(cond[0])-1
    oper = cond[1]
    condition = cond[2] 
    #print "cond: ",condition
    temp1 =  my_csv[:, y]
    index_list1 = []
    for i in temp1:
        operx =  oper_dict[oper]
        if operx(i,float(condition)):
            itemindex = np.where(temp1==i)
            index_list1.append(itemindex[0][0])
    #print "index_list: ", index_list1
    return set(index_list1)

def call_function1(cond):
    global my_cols1
    global my_cols2
    index_list1 = []
    table_name = tables[0] + ".csv"
    #print table_name
    my_csv = np.genfromtxt(table_name, delimiter=',')
    x = my_cols1.index(columns[0])-1
    y = my_cols1.index(cond[0])-1
    oper = cond[1]
    condition = cond[2] 
    temp1 = my_csv[:,y]
    #print "temp1: ", temp1
    for i in temp1:
        operx =  oper_dict[oper]
        if  operx(i,float(condition)) :
            itemindex = np.where(temp1==i)
            index_list1.append(itemindex[0][0])
    #print "index_list: ", index_list1
    return set(index_list1)

def call_function2(cond):
    global my_cols1
    global my_cols2
    global distinct_flag
    index_list1 = []
    table_name = tables[0] + ".csv"
    my_csv = np.genfromtxt(table_name, delimiter=',')
    temp1 = []
    index_list1 = []
    y = my_cols1.index(cond[0])-1
    oper = cond[1]
    condition = cond[2] 
    temp1 = my_csv[:,y]
    if distinct_flag == 0:
        for j in temp1:
            operx =  oper_dict[oper]
            if  operx(j,float(condition)) :
                itemindex = np.where(temp1==j)
                index_list1.append(itemindex[0][0])
    #print "index_list: ", index_list1
    return set(index_list1)

def myfunc(index_list):
    global where_flag
    temp2 = []
    global sum_flag, max_flag, min_flag, avg_flag,and_flag,or_flag
    table_name = tables[0] + ".csv"
    my_csv = np.genfromtxt(table_name, delimiter=',')
    x = my_cols1.index(columns[0])-1
    if distinct_flag == 0:
        if sum_flag == 0  and max_flag == 0 and min_flag == 0 and avg_flag == 0:
            for i in index_list:
                print my_csv[i][x]
        if sum_flag == 1:
            sum_list = []
            for i in index_list:
                sum_list.append(my_csv[i][x])
            print np.sum(sum_list)
        if max_flag == 1:
            sum_list = []
            for i in index_list:
                sum_list.append(my_csv[i][x])
            print np.max(sum_list)
        if min_flag == 1:
            sum_list = []
            for i in index_list:
                sum_list.append(my_csv[i][x])
            print np.min(sum_list)
        if avg_flag == 1:
            sum_list = []
            for i in index_list:
                sum_list.append(my_csv[i][x])
            print np.mean(sum_list)
    elif distinct_flag == 1:
        for i in index_list:
            if my_csv[i][x] not in temp2:
                print my_csv[i][x]
                temp2.append(my_csv[i][x])

def myfunc3(ind):
    table_name = tables[0] + ".csv"
    my_csv = np.genfromtxt(table_name, delimiter=',')
    temp2 = set()
    if distinct_flag == 0:
        for i in ind:
             if tuple(my_csv[i]) not in temp2:
                pprint(my_csv[i])
                temp2.add(tuple(my_csv[i]))
    else:
        for i in ind:
            if tuple(my_csv[i]) not in temp2:
                pprint(my_csv[i])
                temp2.add(tuple(my_csv[i]))    


def myfunc1(index_list):
    global where_flag
    global sum_flag, max_flag, min_flag, avg_flag,and_flag,or_flag
    table_name = tables[0] + ".csv"
    my_csv = np.genfromtxt(table_name, delimiter=',')
    if distinct_flag == 0:
        for j in index_list:
            for i in columns:
                x = my_cols1.index(i)-1
                print my_csv[j][x],
            print

    if distinct_flag == 1:
        for j in index_list:
            temp2 = set()
            for i in columns:
                x = my_cols1.index(i)-1
                temp2.add(my_csv[j][x])
            #print temp2
            if tuple(temp2) not in temp3:
                temp3.add(tuple(temp2))
                qvar.append(temp2)
        pprint(qvar)

def integrated():
    global oper_dict,left_cond,right_cond
    global my_cols1
    global my_cols2
    global where_flag

    table_star = []
    table_name1 = tables[0] + ".csv"
    my_csv1 = np.genfromtxt(table_name1, delimiter=',')
    table_name2 = tables[1] + ".csv"
    #print table_name2
    my_csv2 = np.genfromtxt(table_name2, delimiter=',')
    # print "1: ", my_csv1
    # print "2: ", my_csv2 
    table_star = np.array([np.hstack((i, j)) for i in my_csv1 for j in my_csv2])    
    #table_star[table_star[:,colA] == table_star[:, colB]]
    
    if columns[0] == '*':
        if where_flag == 0:
            print table_star
        elif where_flag == 1:
            if or_flag == 0 or and_flag ==0:
                temp1 = []
                temp2 = []
                y = my_cols1.index(whereConds[0].split(".")[1])
                oper = whereConds[1]
                x = my_cols2.index(whereConds[2].split(".")[1])+len(my_cols1)

                temp1 = table_star[:, y]
                temp2 = table_star[:, x]
                index_list1 = []
                for i, (x_i,x_j) in enumerate(zip(temp1,temp2)):
                    operx =  oper_dict[oper]
                    if operx(x_i,x_j):
                        index_list1.append(i)
                temp2 = set()
                if distinct_flag == 0:
                    for i in index_list1:
                         if tuple(table_star[i]) not in temp2:
                            pprint(table_star[i])
                            temp2.add(tuple(table_star[i]))
                else:
                    for i in index_list1:
                        if tuple(table_star[i]) not in temp2:
                            pprint(table_star[i])
                            temp2.add(tuple(table_star[i]))
    elif len(columns) >= 1:
            
        if where_flag == 0:
            pvar = set()
            qvar = []
            if distinct_flag == 0:
                for i in columns:
                    x = my_cols1.index(i)
                    qvar.append(table_star[:, x])
                pprint(np.array(qvar).T)
            else:
                for i in columns:
                    x = my_cols1.index(i)-1
                    if tuple(table_star[:, x]) not in pvar:
                        pvar.add(tuple(table_star[:, x]))
                        qvar.append(table_star[:, x])
                pprint(np.array(qvar).T)


        elif where_flag == 1:
            if or_flag == 0 and and_flag == 0:
                temp1 = []
                temp2 = []
                temp3 = set()
                qvar = []
                index_list1 = []
                y = my_cols1.index(whereConds[0].split(".")[1])
                oper = whereConds[1]
                x = my_cols2.index(whereConds[2].split(".")[1])+len(my_cols1)
                temp1 = table_star[:,y]
                temp2 = table_star[:,x]
                
                for i, (x_i,x_j) in enumerate(zip(temp1,temp2)):
                    operx =  oper_dict[oper]
                    if operx(x_i,x_j):
                        index_list1.append(i)
                #print "in: ", index_list1
                if distinct_flag == 0:
                    for j in index_list1:
                        for i in columns:
                            x = my_cols1.index(i)
                            print table_star[j][x],
                        print

                if distinct_flag == 1:
                    for j in index_list1:
                        temp4 = set()
                        for i in columns:
                            x = my_cols1.index(i)
                            temp4.add(table_star[j][x])
                        if tuple(temp4) not in temp3:
                            temp3.add(tuple(temp4))
                            qvar.append(temp4)
                    pprint(qvar)
           


def execute():
    global oper_dict,left_cond,right_cond
    global my_cols1
    global my_cols2
    global where_flag
    global distinct_flag
    global sum_flag, max_flag, min_flag, avg_flag,and_flag,or_flag
    my_cols1 = meta_cols[0][0:]
    my_cols2 = meta_cols[1][0:]
    #print "or_flag", or_flag
    # print "mycols2: ", my_cols2
    #print "whre: ", where_flag
    temp1 = []
    index_list = []
    if len(tables) == 1:
        if len(columns) == 1:
            table_name = tables[0] + ".csv"
            print table_name
            my_csv = np.genfromtxt(table_name, delimiter=',')
            #print "my_csv: ", my_csv
            if columns[0] == '*':
                if where_flag == 0 and distinct_flag == 0:
                    pprint(my_csv)
                elif where_flag == 0 and distinct_flag == 1:
                    temp2 = set()
                    for c_i in my_csv:
                        if tuple(c_i) not in temp2:
                            pprint(c_i)
                            temp2.add(tuple(c_i))
                
                elif where_flag == 1:
                    if or_flag == 0 or and_flag ==0:
                        y = my_cols1.index(whereConds[0])
                        oper = whereConds[1]
                        condition = whereConds[2] 
                        print "cond: ",condition
                        temp1 =  my_csv[:, y]
                        
                        for i in temp1:
                            operx =  oper_dict[oper]
                            if  operx(i,float(condition)) :
                                itemindex = np.where(temp1==i)
                                index_list.append(itemindex[0][0])
                        temp2 = set()
                        if distinct_flag == 0:
                            for i in index_list:
                                 if tuple(my_csv[i]) not in temp2:
                                    pprint(my_csv[i])
                                    temp2.add(tuple(my_csv[i]))
                        else:
                            for i in index_list:
                                if tuple(my_csv[i]) not in temp2:
                                    pprint(my_csv[i])
                                    temp2.add(tuple(my_csv[i]))
                    if or_flag == 1:
                        ind = []
                        left_index = set()
                        right_index = set()
                        left_index = call_function(left_cond)
                        right_index = call_function(right_cond)

                        ind = left_index | right_index
                        #print "ind: ", ind
                        myfunc3(ind)
                    if and_flag == 1:
                        ind = []
                        left_index = set()
                        right_index = set()
                        left_index = call_function(left_cond)
                        right_index = call_function(right_cond)

                        ind = left_index & right_index
                        #print "ind: ", ind
                        myfunc3(ind)

            else:
            #if table_name == "table1.csv":
                if where_flag == 0:
                    if distinct_flag == 0 :
                        if sum_flag == 0  and max_flag == 0 and min_flag == 0 and avg_flag == 0:
                            x = my_cols1.index(columns[0])-1
                            print my_csv[:, x]
                        if sum_flag == 1:
                            x = my_cols1.index(columns[0])-1
                            #print "x: ", x
                            print np.sum(my_csv[:, x])
                        if max_flag == 1:
                            x = my_cols1.index(columns[0]) - 1
                            print np.max(my_csv[:, x])
                        if min_flag == 1:
                            x = my_cols1.index(columns[0])-1
                            print np.min(my_csv[:, x])
                        if avg_flag == 1:
                            x = my_cols1.index(columns[0])-1
                            print np.mean(my_csv[:, x])
                    
                    if distinct_flag == 1:
                        temp2 = set()
                        x = my_cols1.index(columns[0])-1
                        #print"my:", my_csv[:,x]
                        for c_i in my_csv[:,x]:
                            if c_i not in temp2:
                                print c_i
                                temp2.add(c_i)

                    
                elif where_flag == 1:
                    if or_flag == 0 and and_flag == 0:
                        temp2 = []
                        x = my_cols1.index(columns[0])-1
                        y = my_cols1.index(whereConds[0])-1
                        oper = whereConds[1]
                        condition = whereConds[2] 
                        temp1 = my_csv[:,y]
                        #print "temp1: ", temp1
                        for i in temp1:
                            operx =  oper_dict[oper]
                            if  operx(i,float(condition)) :
                                itemindex = np.where(temp1==i)
                                index_list.append(itemindex[0][0])
                        #print "sss:", index_list
                        if distinct_flag == 0:
                            if sum_flag == 0  and max_flag == 0 and min_flag == 0 and avg_flag == 0:
                                for i in index_list:
                                    print my_csv[i][x]
                            if sum_flag == 1:
                                sum_list = []
                                for i in index_list:
                                    sum_list.append(my_csv[i][x])
                                print np.sum(sum_list)
                            if max_flag == 1:
                                sum_list = []
                                for i in index_list:
                                    sum_list.append(my_csv[i][x])
                                print np.max(sum_list)
                            if min_flag == 1:
                                sum_list = []
                                for i in index_list:
                                    sum_list.append(my_csv[i][x])
                                print np.min(sum_list)
                            if avg_flag == 1:
                                sum_list = []
                                for i in index_list:
                                    sum_list.append(my_csv[i][x])
                                print np.mean(sum_list)
                        elif distinct_flag == 1:
                            for i in index_list:
                                if my_csv[i][x] not in temp2:
                                    print my_csv[i][x]
                                    temp2.append(my_csv[i][x])

                    if or_flag == 1:
                        ind = []
                        left_index = set()
                        right_index = set()
                        left_index = call_function1(left_cond)
                        right_index = call_function1(right_cond)
                        ind = left_index | right_index
                        #print "ind: ", ind
                        myfunc(ind)
                    if and_flag == 1:
                        ind = []
                        left_index = set()
                        right_index = set()
                        left_index = call_function1(left_cond)
                        right_index = call_function1(right_cond)
                        ind = left_index & right_index
                        #print "ind: ", ind
                        myfunc(ind)

        
        elif len(columns) > 1:
            table_name = tables[0] + ".csv"
            #print table_name
            my_csv = np.genfromtxt(table_name, delimiter=',')
            
            if where_flag == 0:
                pvar = set()
                qvar = []
                if distinct_flag == 0:
                    for i in columns:
                        x = my_cols1.index(i)-1
                        qvar.append(my_csv[:, x])
                    pprint(np.array(qvar).T)
                else:
                    for i in columns:
                        x = my_cols1.index(i)-1
                        if tuple(my_csv[:, x]) not in pvar:
                            pvar.add(tuple(my_csv[:, x]))
                            qvar.append(my_csv[:, x])
                    pprint(np.array(qvar).T)


            elif where_flag == 1:
                if or_flag == 0 and and_flag == 0:
                    temp1 = []
                    #temp2 = set()
                    temp3 = set()
                    qvar = []
                    y = my_cols1.index(whereConds[0])-1
                    oper = whereConds[1]
                    condition = whereConds[2] 
                    temp1 = my_csv[:,y]
                    
                    for j in temp1:
                            operx =  oper_dict[oper]
                            if  operx(j,float(condition)) :
                                itemindex = np.where(temp1==j)
                                index_list.append(itemindex[0][0])
                    if distinct_flag == 0:
                        for j in index_list:
                            for i in columns:
                                x = my_cols1.index(i)
                                print my_csv[j][x],
                            print

                    if distinct_flag == 1:
                        for j in index_list:
                            temp2 = set()
                            for i in columns:
                                x = my_cols1.index(i)
                                temp2.add(my_csv[j][x])
                            #print temp2
                            if tuple(temp2) not in temp3:
                                temp3.add(tuple(temp2))
                                qvar.append(temp2)
                        pprint(qvar)
                if or_flag == 1:
                    ind = []
                    left_index = set()
                    right_index = set()
                    left_index = call_function1(left_cond)
                    right_index = call_function1(right_cond)
                    ind = left_index | right_index
                   # print "ind: ", ind
                    myfunc1(ind)
                if and_flag == 1:
                    ind = []
                    left_index = set()
                    right_index = set()
                    left_index = call_function1(left_cond)
                    right_index = call_function1(right_cond)
                    ind = left_index & right_index
                    #print "ind: ", ind
                    myfunc1(ind)

    elif len(tables) > 1:
        integrated()

def parse(q):
    
    global where_flag
    global distinct_flag
    global columns,left_cond,right_cond
    global sum_flag, max_flag, min_flag, avg_flag,and_flag,or_flag
    operations = []
    quer = str(sqlparse.format(q, reindent=False, keyword_case='lower'))
    query = (quer).split(" ")
    #print "query: ", query
    #i = query.count('')
    cnt = len(query)

    for i in query:
        if i == "select" or i == "from" or i == "where":
            temp1 = i
            query[query.index(i)] = temp1
            operation = query[query.index(i)]
            #print "op: ", operation
            operations.append(operation)
    #print "operation: ", operations
    query[1] = re.sub(r'[)(,]',r' ', query[1])
    #print "q: ",query
    index_cols = query.index('select')
    index_tbls = query.index('from')
    index_cols += 1

    while index_cols != index_tbls:
        columns.extend(query[index_cols].split(" "))         
        index_cols += 1
    
    #print "cols: ", columns
    attributes = columns[0].split("(")
    #print "attr: ", attributes
    attributes.extend(columns[1:-1])
    attributes.extend(columns[-1].split(")"))
    #print "attr: ", attributes
    if 'distinct' in attributes:
        distinct_flag = 1
    if 'sum' in attributes:
        sum_flag = 1
    if 'min' in attributes:
        min_flag = 1
    if 'max' in attributes:
        max_flag = 1
    if 'avg' in attributes:
        avg_flag = 1
    if distinct_flag == 1 or sum_flag == 1 or min_flag == 1 or max_flag == 1 or avg_flag == 1:
        columns = attributes[1:-1]
        print "cols: ", columns
    
    if 'where' in query:
        where_flag = 1

    if where_flag == 1:
        index_where = query.index('where')
        index_tbls += 1
        while index_tbls != index_where:
            tables.extend(query[index_tbls].split(","))
            index_tbls += 1
    else:
        index_tbls += 1
        tables.extend(query[index_tbls].split(","))
    
    #print "tabls: ", tables
    
    if where_flag == 1:
        for i in query:
            if query.index(i) >= (index_where + 1) :
                whereConds.append(i)
    #print "wherecond: ", whereConds

    if 'or' in whereConds:
        or_flag = 1
        left_cond = whereConds[0:whereConds.index('or')]
        right_cond = whereConds[whereConds.index('or')+1:]
    if 'and' in whereConds:
        and_flag = 1
        left_cond = whereConds[0:whereConds.index('and')]
        right_cond = whereConds[whereConds.index('and')+1:]
    #print "l:", left_cond
    #print "r:", right_cond
def schema():
    
    meta_reader = open('metadata.txt')
    for line in meta_reader.readlines():
        if 'begin_table' in line:
            temp1 = []
            begin_table = 1
        elif 'end_table' in line:
            meta_cols.append(temp1)
            begin_table = 0
        elif begin_table == 1 and 'table' in line:
            meta_table.append(line[:-1])
        elif begin_table == 1 and 'table' not in line:
            temp1.append(line[:-1])
    #print "meta_table: ", meta_table
    #print "meta_cols: ", meta_cols


if __name__ == "__main__":
    q = str(sys.argv[1])
    parse(q)
    schema()
    execute()