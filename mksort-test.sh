#!/usr/bin/env bash

# number of rows in the table
for nrows in 1000 10000 100000 1000000 10000000; do

	# data type for the columns
	for dtype in int bigint timestamptz text; do

		# number of repetitions for each value (ndistinct = nrows/count)
		for count in 1 5 10 25 50 100 10000 $((nrows/10)) $nrows; do

			if [ "$count" -gt "$nrows" ]; then
				continue
			fi

			# data distribution for the columns
			for distribution in random correlated sequential; do

				# number of columns
				for ncols in 1 2 3 4 5 6 7 8; do

					# Generate a table with the specified number of columns
					# and data type. The columns have a given data distribution
					# and number of repetitions of each value.
					#
					# Not executed directly, but writes a SQL script in the
					# "sql" directory to make it easier to reproduce.

					psql test -c "drop table if exists t" >> debug.log 2>&1

					echo "create table t (" > create.sql

					for c in $(seq 1 $ncols); do
						echo "c$c $dtype" >> create.sql
						if [ "$c" != "$ncols" ]; then
							echo ", " >> create.sql
						fi
					done

					echo ");" >> create.sql

					expr=""

					if [ "$distribution" == "random" ]; then
						if [ "$dtype" == "int" ]; then
							expr="(($nrows / $count) * random())"
						elif [ "$dtype" == "bigint" ]; then
							expr="(($nrows / $count) * random())"
						elif [ "$dtype" == "timestamptz" ]; then
							expr="(now() + format('%s days', 1 + (($nrows / $count) * random())::int)::interval)"
						elif [ "$dtype" == "text" ]; then
							expr="((($nrows / $count) * random())::int::text)"
						fi
					elif [ "$distribution" == "correlated" ]; then
						if [ "$dtype" == "int" ]; then
							expr="((i / $count) + random())"
						elif [ "$dtype" == "bigint" ]; then
							expr="((i / $count) + random())"
						elif [ "$dtype" == "timestamptz" ]; then
							expr="(now() + format('%s days', 1 + ((i/$count) + random())::int)::interval)"
						elif [ "$dtype" == "text" ]; then
							expr="(((i / $count) + random())::int::text)"
						fi
					elif [ "$distribution" == "sequential" ]; then
						if [ "$dtype" == "int" ]; then
							expr="((i / $count))"
						elif [ "$dtype" == "bigint" ]; then
							expr="((i / $count))"
						elif [ "$dtype" == "timestamptz" ]; then
							expr="(now() + format('%s days', 1 + ((i/$count)))::interval)"
						elif [ "$dtype" == "text" ]; then
							expr="((i / $count)::int::text)"
						fi
					fi

					echo "insert into t select " >> create.sql

					for c in $(seq 1 $ncols); do
						echo "$expr" >> create.sql
						if [ "$c" != "$ncols" ]; then
							echo ", " >> create.sql
						fi
					done

					echo "from generate_series(1,$nrows) s(i);" >> create.sql

					echo 'vacuum analyze t;' >> create.sql
					echo 'checkpoint;' >> create.sql

					cp create.sql sql/$nrows-$dtype-$count-$distribution-$ncols.sql

					# now actually run the generated SQL script
					psql test < create.sql >> debug.log 2>&1

					ndistinct1=$((nrows/count))
					ndistinct2=$(psql -t -A test -c 'select count(distinct c1) from t')

					# generate the ORDER BY query
					query="SELECT * FROM t ORDER BY "

					for c in $(seq 1 $ncols); do
						query="$query $c "
						if [ "$c" != "$ncols" ]; then
							query="$query ,"
						fi
					done

					echo "EXPLAIN (ANALYZE, TIMING OFF) $query" >> explain-on.log;
					echo "EXPLAIN (ANALYZE, TIMING OFF) $query" >> explain-off.log;

					# run the query 3x for each GUC value
					for r in 1 2 3; do

						for mksort in off on; do

							psql test > timing.log <<EOF
SET max_parallel_workers_per_gather = 0 ;
SET work_mem = '1GB';
SET enable_mk_sort = '$mksort';
EXPLAIN (ANALYZE, TIMING OFF) $query;
EOF

							t=$(grep 'Execution Time' timing.log | awk '{print $3}')

							echo "===== rows $nrows type $dtype count $count distribution $distribution cols $ncols run $r =====" >> explain-$mksort.log 2>&1
							cat timing.log >> explain-$mksort.log 2>&1

							echo $nrows $dtype $distribution $count $ndistinct1 $ndistinct2 $ncols $r $mksort $t

						done

					done

				done

			done

		done

	done

done

