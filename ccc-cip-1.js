const { Client } = require("pg");

const defaultTableStyles = {
    table: {
      border: "1px solid black",
      "border-collapse": "collapse",
    },
    thead: {
      "background-color": "#008000",
    },
    th: {
      "border": "1px solid black",
      padding: "5px",
    },
    tbody: {
      "background-color": "white",
    },
    td: {
      "border": "1px solid black",
      padding: "5px",
    },
  };

  const customStyles = {
    table: {
      border: "1px solid black",
      "border-collapse": "collapse",
    },
    thead: {
      "background-color": "#008000",
    },
    th: {
      "border": "1px solid black",
      padding: "5px",
    },
    tbody: {
      "background-color": "white",
    },
    td: {
      "border": "1px solid black",
      padding: "5px",
    },
  };

const baityPackages = ["download_speed", "local_and_international_calls", "monthly_data_capacity", "monthly_subscription_fees", "other", "service", "service_contract", "shahid", "stc_play", "stc_tv_device", "stc_tv_package", "upload_speed", "upload_speed_for_other_users", "wifi_boosters"];
const offers = ["article_link", "offer", "offer_description", "offer_start_and_end_date", "offer_type", "short_description", "tags", "targeted_segment"];
const postpaidPackages = ["additionalservices", "article_link", "internet", "datacapacity", "devicescontractplan", "insidenetworkcalls", "insidenetworkmessage", "internationalcalls", "multiplesims", "offersonthepackage", "otherfeatures", "outsidenetworkcalls", "outsidenetworkmessages", "packageactivationcode", "price1", "price", "roamingcalls", "roamingdata", "service", "socialmediadata", "tamayouz"];
const quicknetPackages = ["apps", "devices", "establishment_cost", "notes", "offers", "package_name", "package_type", "plan_type", "price1", "price", "security_deposit", "validity"];
const sawaPackages = ["activation_code", "calls", "data", "links", "offers", "price", "price1", "service", "social", "wifi"];
const services = ["activation_code", "article_link", "cancellation_code", "category", "fees", "service", "service_details"];

function buildSearchWhereClause(inputParamName, inputParamValue, searchList) {
    if (!searchList || !searchList.length) {
      throw new Error("Search list cannot be empty");
    }
  
    const conditions = [];
  
    for (const searchString of searchList) {
      console.log(searchString);
      if (searchString.includes(inputParamName)) {
        conditions.push(` LOWER(${searchString}) ILIKE '%${inputParamValue.toLowerCase()}%' `);
        //break; // Stop iterating after the first match
      }
    }
  
    const whereClause = conditions.length ? ` (${conditions.join(" OR ")})` : "";
  
    return whereClause;
  }

function buildRangeWhereClause(paramName, paramValue) {
    const rangeValues = paramValue.split("|");
  
    const minVal = rangeValues[0] ? rangeValues[0].trim() : null; // Handle trimming and null
    const maxVal = rangeValues[1] ? rangeValues[1].trim() : null; // Handle trimming and null
  
    let whereClause = "";
  
    // **Choose either regular expression or casting approach based on your needs:**
  
    // **Regular Expression Approach (if supported by PostgreSQL):**
    // Uncomment the following line if you want to use regular expressions
    // if (minVal) {
    //   whereClause += ` ${paramName} REGEXP '^[0-9]{1,}$' AND `;
    // }
  
    // **Casting Approach (widely supported):**
    if (minVal !== null) {
      whereClause += ` CAST(${paramName} AS FLOAT) >= ${minVal}`;
    }
    if (maxVal !== null) {
      whereClause += (whereClause.length > 0 ? " AND " : " ") + `CAST(${paramName} AS FLOAT) <= ${maxVal}`;
    }

    console.log(whereClause);
  
    return whereClause;
}
  
function findTable(tableName) {
    switch (tableName.toLowerCase()) { // Convert to lowercase for case-insensitive matching
        case "services":
            return services;
        case "baity_packages":
            return baityPackages;
        case "sawa_packages":
            return sawaPackages;
        case "postpaid_packages":
            return postpaidPackages;
        case "offers":
            return offers;
        case "quicknet_packages":
            return quicknetPackages;
        default:
            return "";
    }
}

async function SELECT_FROM_DB(filterParams) {
  const { tableName, filterParamNames, filterParamValues } = filterParams;

  // Validate parameter lengths
  if (filterParamNames.split(",").length !== filterParamValues.split(",").length) {
    throw new Error("Number of filter parameter names must match number of values");
  }

  const dbConfig = {
    user: "postgres",
    host: "localhost",
    database: "postgres",
    password: "admin",
    port: 5432,
  };

    //const dbConfig = {
            //user: "postgres",
            //host: "pgm-l4vvavi13uymn3d8.pgsql.me-central-1.rds.aliyuncs.com",
            //database: "db1",
           // password: "jH2zDxLTKx",
         //   port: 5432
       // };

    const client = new Client(dbConfig);

    try {
        await client.connect();

        const paramNames = filterParamNames.split(",");
        const paramValues = filterParamValues.split(",");
        const tableToSearch = findTable(tableName);
        console.log(tableToSearch);
        if(tableToSearch === "") {
            return false;
        }

        let whereClause = "";

        for (let i = 0; i < paramNames.length; i++) {
            const paramName = paramNames[i];
            const paramValue = paramValues[i];

            // Check if the value contains a range delimiter
            if(paramName.includes("shahid","stc tv")) {
                whereClause += (whereClause.length >0 ? " AND " : " WHERE ") + `LOWER(${paramName}) != 'not includded'`;
            } else if (paramValue.includes("|")) {
                whereClause += (whereClause.length > 0 ? " AND " : " WHERE ") + buildRangeWhereClause(paramName, paramValue);
            } else {
                whereClause += (whereClause.length > 0 ? " AND " : " WHERE ") + buildSearchWhereClause(paramName, paramValue, tableToSearch);
            }
        }

        const finalQuery = `SELECT * FROM ${tableName}` + whereClause;

        //console.log(finalQuery);
        //console.log(paramValues);

        const res = await client.query(finalQuery);

        client.end();
        const data = res.rows;
        const tableNamesToTranspose = ["postpaid_packages", "baity_packages"];
        const shouldTranspose = tableNamesToTranspose.includes(filterParams.tableName); // Check for match
        console.log("####################################");
        console.log(data);
        console.log("####################################");
        if(shouldTranspose) {
            const transposedData = transpose(data, "service");
            const transposedHtmlTable = createHTMLTableFromTransposedData(transposedData);
            return { data: transposedData, transposedHtmlTable };
        } 
        const htmlTable = createHTMLTable(data); // Call createHTMLTable
         // Return data and HTML table string
         return { data: data, htmlTable };
    } catch (e) {
        console.error("Error:", e);
        return { status: 500, message: "Error fetching data" };
    }
}

function transpose(data, headerColumn) {
    const transposedData = {};
    // Extract headers from the specified column
    const headers = data.map(row => row[headerColumn]);

    // Remove the header column from data (optional)
    const dataWithoutHeaders = data.map(row => {
        const newRow = { ...row };
        delete newRow[headerColumn];
        return newRow;
    });

    // Build transposed structure
    for (const row of dataWithoutHeaders) {
        for (const [key, value] of Object.entries(row)) {
            transposedData[key] = transposedData[key] || [];
            transposedData[key].push(value);
        }
    }

    // Combine headers with transposed data
    return { headers, ...transposedData };
}

function createHTMLTable(data) {

    const columnsToIgnore = ["service_id", "created_time", "modified_time", "lang", "created_by", "createdby"]; // Example columns to ignore

    if (data.length === 0) {
        return "<p>No results found.</p>";
      }
    
      let htmlTable = "<table style='border: 1px solid black; border-collapse: collapse;'>";
      htmlTable += "<thead><tr style='background-color: #008000;'>";
    console.log("------------------------");
      console.log(data);
      console.log("------------------------");
      // Filter column names excluding ignored columns
      const tableHeaders = Object.keys(data[0]).filter(
        (key) => !columnsToIgnore.includes(key)
      );
    
      // Add table headers dynamically
      for (const key of tableHeaders) {
        htmlTable += `<th style='border: 1px solid black; padding: 5px;'>${key}</th>`;
      }
    
      htmlTable += "</tr></thead><tbody>";
    
      for (const row of data) {
        htmlTable += "<tr style='background-color: white;'>";
        // Filter data for each row excluding ignored columns
        const filteredData = Object.entries(row).filter(
          ([key]) => !columnsToIgnore.includes(key) // Filter by key (column name)
        );
    
        // Extract only data values from filtered entries
        const tableData = filteredData.map(([key, value]) => value);  
    
        for (const value of tableData) {
          htmlTable += `<td style='border: 1px solid black; padding: 5px;'>${value}</td>`;
        }
        htmlTable += "</tr>";
      }
    
      htmlTable += "</tbody></table>";
      return htmlTable;
  }

  function createHTMLTableFromTransposedData(transposedData) {
    const columnsToIgnore = ["service_id", "created_time", "modified_time", "lang", "created_by", "createdby", "modifieddate","createddate"];
console.log(transposedData)
    // Filter column names excluding ignored columns
    const tableHeaders = Object.values(transposedData.headers || {}).filter(
      (key) => !columnsToIgnore.includes(key)
    );
  
    // Build the HTML table string
    let htmlTable = "";
    htmlTable += `<table style="${applyStyles(defaultTableStyles.table, customStyles.table)}">`;
    htmlTable += "<thead><tr style='background-color: #008000;'>";
    console.log("++++++++++++++++++");
    console.log(tableHeaders);
    console.log("++++++++++++++++++");
    // Add table headers dynamically
    for (const key of tableHeaders) {
      htmlTable += `<th style='border: 1px solid black; padding: 5px;'>${key}</th>`;
    }
  
    htmlTable += "</tr></thead><tbody>";
  
    // Create data rows
    for (const key in transposedData) {
      if (key !== "headers") { // Avoid processing headers again
        htmlTable += "<tr style='background-color: white;'>";
  
        // Filter data for each row excluding ignored columns
        const filteredData = transposedData[key].filter(
          (value, index) => !columnsToIgnore.includes(tableHeaders[index]) // Filter by index using headers
        );
  
        // Add data values to the row
        for (const value of filteredData) {
          htmlTable += `<td style='border: 1px solid black; padding: 5px;'>${value}</td>`;
        }
  
        htmlTable += "</tr>";
      }
    }
  
    htmlTable += "</tbody></table>";
  
    return htmlTable;
  }
  

  function applyStyles(defaultStyle, overrideStyle) {
    let styleString = "";
    for (const key in defaultStyle) {
      styleString += `${key}: ${defaultStyle[key]};`;
    }
    if (overrideStyle) {
      for (const key in overrideStyle) {
        styleString += `${key}: ${overrideStyle[key]};`;
      }
    }
    return styleString;
  }
  

// Example usage (assuming filter parameters are provided as strings)
const filterParams = {
  tableName: "baity_packages",
  filterParamNames: "shahid,stc",
  filterParamValues: ",stc",
};

SELECT_FROM_DB(filterParams)
  .then((response) => console.log(response))
  .catch((error) => console.error(error));
