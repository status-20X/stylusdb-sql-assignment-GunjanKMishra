function parseJoinClause(query) {
    const joinRegex =
        /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim(),
            },
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null,
    };
}

function parseInsertQuery(query) {
    query = query.replace(/"?\w+"?\."(\w+)"?/g, "$1");

    const insertRegex =
        /INSERT INTO "?(\w+)"?\s\(([^)]+)\)\sVALUES\s\(([^)]+)\)/i;
    const insertMatch = query.match(insertRegex);

    if (!insertMatch) {
        throw new Error("Invalid INSERT INTO syntax.");
    }

    const [, table, columns, values] = insertMatch;

    const parsedColumns = columns.split(",").map((name) => {
        return name.trim().replace(/^"?(.+?)"?$/g, "$1");
    });

    const parsedValues = values.split(",").map((value) => {
        return value
            .trim()
            .replace(/^'(.*)'$/g, "$1")
            .replace(/^"(.*)"$/g, "$1");
    });

    const returningMatch = query.match(/RETURNING\s(.+)$/i);
    const returningColumns = returningMatch
        ? returningMatch[1].split(",").map((name) => {
            return name
                .trim()
                .replace(/\w+\./g, "")
                .replace(/^"?(.+?)"?$/g, "$1");
        })
        : [];
    return {
        type: "INSERT",
        table: table.trim().replace(/^"?(.+?)"?$/g, "$1"),
        columns: parsedColumns,
        values: parsedValues,
        returningColumns,
    };
}

function parseDeleteQuery(query) {
    const deleteRegex = /DELETE FROM (\w+)( WHERE (.*))?/i;
    const deleteMatch = query.match(deleteRegex);

    if (!deleteMatch) {
        throw new Error("Invalid DELETE syntax.");
    }

    const [, table, , whereString] = deleteMatch;
    let whereClause = [];
    if (whereString) {
        whereClause = parseWhereClause(whereString);
    }

    return {
        type: "DELETE",
        table: table.trim(),
        whereClause,
    };
}

module.exports = {
    parseSelectQuery,
    parseJoinClause,
    parseInsertQuery,
    parseDeleteQuery,
};