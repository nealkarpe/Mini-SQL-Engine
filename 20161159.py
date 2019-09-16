import csv, sys, sqlparse, itertools

def getTables(tables, table_attributes):
	f = open('metadata.txt','r')
	readTableName = True
	curr_table = ""
	attribute_names = []
	for line in f:
		line = line.strip()
		if line == "<begin_table>":
			readTableName = True
		elif line == "<end_table>":
			with open(curr_table+".csv") as csvfile:
				rows = csv.reader(csvfile)
				for row in rows:
					for i in range(len(row)):
						tables[curr_table][attribute_names[i]].append(int(row[i]))
			table_attributes[curr_table] = attribute_names
			if len(attribute_names) == 0:
				exit("Error: Table must have at least one column")
		elif readTableName:
			curr_table = line
			tables[curr_table] = {}
			attribute_names = []
			readTableName = False
		else:
			tables[curr_table][line] = []
			attribute_names.append(line)		

class Comparison():
	def __init__(self, lhs, comp, rhs):
		if isinstance(lhs, sqlparse.sql.Identifier):
			self.lhs_isCol = True
			self.lhs_name = str(lhs)
		elif isinstance(lhs, sqlparse.sql.Token):
			self.lhs_isCol = False
			self.lhs_val = int(str(lhs))
		else:
			exit("Error: incorrect format for condition")
		if isinstance(rhs, sqlparse.sql.Identifier):
			self.rhs_isCol = True
			self.rhs_name = str(rhs)
		elif isinstance(rhs, sqlparse.sql.Token):
			self.rhs_isCol = False
			self.rhs_val = int(str(rhs))
		else:
			exit("Error: incorrect format for condition")
		self.operator = str(comp)

database = {}
table_attributes = {}
getTables(database, table_attributes)

if len(sys.argv) == 1 or sys.argv[1]=="":
	exit("Error: No query statement given")

query = sqlparse.parse(sys.argv[1])[0]

if str(query.tokens[0]).upper() != "SELECT":
	exit("Error: Only SELECT query is implemented")
if str(query.tokens[-1]) != ";":
	if not (isinstance(query.tokens[-1], sqlparse.sql.Where) and str(query.tokens[-1].tokens[-1]) == ";"):
		exit("Error: Query is not in a supported format (or not terminated with semi-colon)")
if len(query.tokens) < 8:
	exit("Error: query not supported")

distinct_flag = False
if str(query.tokens[2]).upper() == "DISTINCT":
	distinct_flag = True
	query.tokens = query.tokens[:2] + query.tokens[4:]

query_cols = []
query_tables = []
comparisons = []

def check_col(col):
	if "." in col:
		components = col.split(".")
		if len(components) > 2:
			exit("Error: table/column names cannot have . character")
		if components[0] not in query_tables:
			exit("Error: table '"+components[0]+"' not included in referenced tables")
		if components[1] not in database[components[0]]:
			exit("Error: table '"+components[0]+"' has no column '"+components[1]+"'")
	else:
		count = 0
		for table in query_tables:
			if col in database[table]:
				count+=1
		if count!=1:
			exit("Error: '"+col+"' must exist in exactly ONE of the referenced tables")

def check_identifiers():
	for table in query_tables:
		if table not in database:
			exit("Error: '" + table + "' - no such table exists")
	for col in query_cols:
		check_col(col)
def check_comparisons():
	for comp in comparisons:
		if comp.lhs_isCol:
			check_col(comp.lhs_name)
		if comp.rhs_isCol:
			check_col(comp.rhs_name)

def remove_col(col):
	ind = -1
	for i in range(len(query_cols)):
		if query_cols[i] == col:
			ind = i
			break
	if ind!=-1:
		query_cols.pop(ind)

def compare(comp,table_rowind):
	if comp.lhs_isCol:
		components = comp.lhs_name.split(".")
		table = components[0]
		col = components[1]
		val1 = database[table][col][table_rowind[table]]
	else:
		val1 = comp.lhs_val
	if comp.rhs_isCol:
		components = comp.rhs_name.split(".")
		table = components[0]
		col = components[1]
		val2 = database[table][col][table_rowind[table]]
	else:
		val2 = comp.rhs_val
	if comp.operator == '=':
		return val1 == val2
	elif comp.operator == '<':
		return val1 < val2
	elif comp.operator == '>':
		return val1 > val2
	elif comp.operator == '<=':
		return val1 <= val2
	elif comp.operator == '>=':
		return val1 >= val2
	else:
		exit("Error: invalid operator - "+comp.operator)

if isinstance(query.tokens[2], sqlparse.sql.Function):
	if len(query.tokens)!=8 or str(query.tokens[4]).upper()!="FROM":
		exit("Error: Incorrect format for aggregate query")
	valid_functions = {"MAX", "MIN", "SUM", "AVERAGE"}
	function = query.tokens[2].get_name().upper()
	if function not in valid_functions:
		exit("Error: '" + function + "' - aggregate function not implemented")
	if not isinstance(query.tokens[6], sqlparse.sql.Identifier):
		exit("Error: Aggregate function - expected type for table is sqlparse.sql.Identifier. Type given is "+str(type(query.tokens[6])))
	table = str(query.tokens[6])
	query_tables.append(table)
	if len(list(query.tokens[2].get_parameters())) != 1:
		exit("Error: Aggregate function should be on exactly one column")
	col = str(query.tokens[2].get_parameters()[0])
	query_cols.append(col)
	check_identifiers()
	if "." in col:
		components = col.split(".")
		col = components[1]
	print(query.tokens[2].get_name() + "(" + table + "." + col + ")")
	if function == "MAX":
		print(max(database[table][col]))
	elif function == "MIN":
		print(min(database[table][col]))
	elif function == "SUM":
		print(sum(database[table][col]))
	elif function == "AVERAGE":
		print(sum(database[table][col])/len(database[table][col]))

elif len(query.tokens)==8:
	if str(query.tokens[4]).upper()!="FROM":
		exit("Error: Incorrect format for select query")
	if isinstance(query.tokens[6], sqlparse.sql.Identifier) or isinstance(query.tokens[6], sqlparse.sql.IdentifierList):
		if isinstance(query.tokens[6], sqlparse.sql.Identifier):
			query_tables.append(str(query.tokens[6]))
		else:
			query_tables = list(map(str,query.tokens[6].get_identifiers()))
		check_identifiers()
		if str(query.tokens[2])=="*":
			for table in query_tables:
				for col in table_attributes[table]:
					query_cols.append(table+"."+col)
		elif isinstance(query.tokens[2],sqlparse.sql.Identifier):
			query_cols.append(str(query.tokens[2]))
		elif isinstance(query.tokens[2],sqlparse.sql.IdentifierList):
			query_cols = list(map(str,query.tokens[2].get_identifiers()))
		else:
			exit("Error: expected Indetifier type after SELECT. Got type- "+str(type(query.tokens[2])))
		check_identifiers()
		indices = []
		for table in query_tables:
			table_size = len(database[table][table_attributes[table][0]])
			indices.append(list(range(table_size)))
		op_rows = itertools.product(*indices)
		for i in range(len(query_cols)):
			col = query_cols[i]
			if "." not in col:
				for table in query_tables:
					if col in database[table]:
						query_cols[i] = table+"."+col
						break
		print(",".join(query_cols))
		already_printed = set()
		for op_row in op_rows:
			table_rowind = {}
			for i in range(len(query_tables)):
				table_rowind[query_tables[i]] = op_row[i]
			vals = []
			for col in query_cols:
				components = col.split(".")
				rowind = table_rowind[components[0]]
				vals.append(str(database[components[0]][components[1]][rowind]))
			to_print = ",".join(vals)
			if distinct_flag and (to_print in already_printed):
				continue
			print(to_print)
			already_printed.add(to_print)
	else:
		exit("Error: expected Indetifier type after FROM. Got type- "+str(type(query.tokens[6])))

elif len(query.tokens)==9 and isinstance(query.tokens[-1], sqlparse.sql.Where):
	if str(query.tokens[4]).upper()!="FROM":
		exit("Error: Incorrect format for select query")
	where = query.tokens[-1]
	if isinstance(query.tokens[6], sqlparse.sql.Identifier):
		query_tables.append(str(query.tokens[6]))
	else:
		query_tables = list(map(str,query.tokens[6].get_identifiers()))
	check_identifiers()
	if str(query.tokens[2])=="*":
		for table in query_tables:
			for col in table_attributes[table]:
				query_cols.append(table+"."+col)
	elif isinstance(query.tokens[2],sqlparse.sql.Identifier):
		query_cols.append(str(query.tokens[2]))
	elif isinstance(query.tokens[2],sqlparse.sql.IdentifierList):
		query_cols = list(map(str,query.tokens[2].get_identifiers()))
	else:
		exit("Error: expected Indetifier type after SELECT. Got type- "+str(type(query.tokens[2])))
	check_identifiers()
	indices = []
	for table in query_tables:
		table_size = len(database[table][table_attributes[table][0]])
		indices.append(list(range(table_size)))
	op_rows = itertools.product(*indices)
	for i in range(len(query_cols)):
		col = query_cols[i]
		if "." not in col:
			for table in query_tables:
				if col in database[table]:
					query_cols[i] = table+"."+col
					break
	if len(where.tokens)==4:
		if not isinstance(where.tokens[2],sqlparse.sql.Comparison):
			exit("Error: invalid comparison - "+str(where.tokens[2]))
		where.tokens[2] = [x for x in where.tokens[2] if not x.is_whitespace]
		comparisons.append(Comparison(where.tokens[2][0],where.tokens[2][1],where.tokens[2][2]))
	elif len(where.tokens)==8:
		if not isinstance(where.tokens[2],sqlparse.sql.Comparison):
			exit("Error: invalid comparison - "+str(where.tokens[2]))
		where.tokens[2] = [x for x in where.tokens[2] if not x.is_whitespace]
		comparisons.append(Comparison(where.tokens[2][0],where.tokens[2][1],where.tokens[2][2]))
		if not isinstance(where.tokens[6],sqlparse.sql.Comparison):
			exit("Error: invalid comparison - "+str(where.tokens[6]))
		where.tokens[6] = [x for x in where.tokens[6] if not x.is_whitespace]
		comparisons.append(Comparison(where.tokens[6][0],where.tokens[6][1],where.tokens[6][2]))
	else:
		exit("Error: invalid WHERE statement")
	check_comparisons()
	for i in range(len(comparisons)):
		comp = comparisons[i]
		if comp.lhs_isCol and "." not in comp.lhs_name:
			for table in query_tables:
				if comp.lhs_name in database[table]:
					comparisons[i].lhs_name = table+"."+comp.lhs_name
					break
		if comp.rhs_isCol and "." not in comp.rhs_name:
			for table in query_tables:
				if comp.rhs_name in database[table]:
					comparisons[i].rhs_name = table+"."+comp.rhs_name
					break
	AND = False
	if len(where.tokens)==8:
		if str(where.tokens[4]).upper() == "AND":
			AND = True
		elif str(where.tokens[4]).upper() == "OR":
			AND = False
		else:
			exit("Error: invalid condition joiner - "+str(where.tokens[4]).upper())
	for comp in comparisons:
		if comp.lhs_isCol and comp.rhs_isCol:
			# if comp.operator != "=":
			# 	exit("Error: invalid operator for join - "+comp.operator)
			if comp.operator == "=" and comp.lhs_name != comp.rhs_name and (comp.lhs_name in query_cols and comp.rhs_name in query_cols):
				remove_col(comp.rhs_name)
		elif (not comp.lhs_isCol) and (not comp.rhs_isCol):
			exit("Error: invalid condition")
	print(",".join(query_cols))

	if len(comparisons) > 2:
		exit("Maximum 2 comparisons are allowed")

	already_printed = set()
	for op_row in op_rows:
		table_rowind = {}
		for i in range(len(query_tables)):
			table_rowind[query_tables[i]] = op_row[i]
		if len(comparisons)==1:
			if not compare(comparisons[0],table_rowind):
				continue
		elif len(comparisons)==2:
			if AND:
				if not (compare(comparisons[0],table_rowind) and compare(comparisons[1],table_rowind)):
					continue
			else:
				if not (compare(comparisons[0],table_rowind) or compare(comparisons[1],table_rowind)):
					continue
		vals = []
		for col in query_cols:
			components = col.split(".")
			rowind = table_rowind[components[0]]
			vals.append(str(database[components[0]][components[1]][rowind]))

		to_print = ",".join(vals)
		if distinct_flag and (to_print in already_printed):
			continue
		print(to_print)
		already_printed.add(to_print)

else:
	exit("Error: invalid query")