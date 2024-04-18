// src/index.js

const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    try {
        const {
            fields,
            table,
            whereClauses,
            joinType,
            joinTable,
            joinCondition,
            groupByFields,
            orderByFields,
            limit,
            isDistinct,
            hasAggregateWithoutGroupBy,
        } = parseSelectQuery(query);

        let data = await readCSV(`${table}.csv`);

        // Perform INNER JOIN if specified
        if (joinTable && joinCondition) {
            const joinData = await readCSV(`${joinTable}.csv`);
            switch (joinType.toUpperCase()) {
                case "INNER":
                    data = performInnerJoin(data, joinData, joinCondition, fields, table);
                    break;
                case "LEFT":
                    data = performLeftJoin(data, joinData, joinCondition, fields, table);
                    break;
                case "RIGHT":
                    data = performRightJoin(data, joinData, joinCondition, fields, table);
                    break;
                // Handle default case or unsupported JOIN types
            }
        }

        let filteredData =
            whereClauses.length > 0
                ? data.filter((row) =>
                    whereClauses.every((clause) => evaluateCondition(row, clause))
                )
                : data;

        // logic for group by
        if (groupByFields) {
            filteredData = applyGroupBy(filteredData, groupByFields, fields);
        }

        if (hasAggregateWithoutGroupBy && fields.length == 1) {
            const selectedRow = {};
            selectedRow[fields[0]] = aggregatedOperations(fields[0], filteredData);
            return [selectedRow];
        }

        // console.log("AFTER GROUP: ", filteredData);

        if (orderByFields) {
            filteredData.sort((a, b) => {
                for (let { fieldName, order } of orderByFields) {
                    if (a[fieldName] < b[fieldName]) return order === "ASC" ? -1 : 1;
                    if (a[fieldName] > b[fieldName]) return order === "ASC" ? 1 : -1;
                }
                return 0;
            });
        }

        // console.log("AFTER ORDER: ", filteredData);

        if (limit !== null) {
            filteredData = filteredData.slice(0, limit);
        }

        if (isDistinct) {
            filteredData = [
                ...new Map(
                    filteredData.map((item) => [
                        fields.map((field) => item[field]).join("|"),
                        item,
                    ])
                ).values(),
            ];
        }

        // Filter the fields based on the query fields
        return filteredData.map((row) => {
            const selectedRow = {};
            fields.forEach((field) => {
                if (hasAggregateWithoutGroupBy) {
                    selectedRow[field] = aggregatedOperations(field, filteredData);
                } else {
                    selectedRow[field] = row[field];
                }
            });
            return selectedRow;
        });
    } catch (error) {
        throw new Error(`Error executing query: ${error.message}`);
    }
}

module.exports = executeSELECTQuery;