# Load necessary libraries
library("admixtools")
library("tidyverse")
library("ggplot2")
library("dplyr")
library("grid")
library("cowplot")
library("future")
library("furrr")
library("parallel")
library("RPostgres")
library("pool")
library("jsonlite")

# Define paths and configurations
prefix_ho <- "c:/datasets/v54.1.p1_HO_public"
progress_log_path <- "c:/qpadmdata/progress.txt"
ongoingCombinationProgress_log_path <- "c:/qpadmdata/ongoing_combination_progress.txt"
all_weights_data_path <- "c:/qpadmdata/all_weights_data.csv"
p_values_path <- "c:/qpadmdata/p_values.csv"
avg_se_path <- "c:/qpadmdata/avg_se.csv"
right_populations_path <- "c:/qpadmdata/right_populations.csv"
rejection_log_path <- "c:/qpadmdata/rejection_log.csv"


# Input strings for right and left populations
input_string_left_static <- "Lebanon_MBA.SG"
input_string_left_dynamic <- "Tajikistan_Ksirov_Kushan,CanaryIslands_Guanche.SG,Italy_PianSultano_BA.SG"
input_string_right_static <- "Mbuti.DG,Israel_PPNB,Russia_MA1_HG.SG,Turkey_Boncuklu_N,Turkey_Epipaleolithic,Morocco_Iberomaurusian,Serbia_IronGates_Mesolithic,Luxembourg_Loschbour.DG,Russia_Karelia_HG,Georgia_Kotias.SG,Iran_GanjDareh_N,China_Tianyuan,Indian_GreatAndaman_100BP.SG,Mongolia_North_N"
input_string_right_dynamic <- "Jordan_PPNB,Russia_Tyumen_HG,Israel_Natufian"


# List of target populations
targets_str <- "Jew_Ashkenazi.HO,Jew_Moroccan.HO"

get_pg_connection_classic <- function() {
  library(RPostgres)
  
  # Define the connection details
  host <- "aws-0-eu-west-3.pooler.supabase.com"
  port <- 6543
  dbname <- "postgres"
  user <- "postgres.zctxxrgkafjrieubmcuz"
  password <- "leACMVQ7MFH9TtFf"  # Replace with your actual password
  
  # Establish a connection to the PostgreSQL database
  con <- dbConnect(RPostgres::Postgres(),
                   dbname = dbname,
                   host = host,
                   port = port,
                   user = user,
                   password = password)
  
  return(con)
}


# Define a connection pool for PostgreSQL
pg_pool <- dbPool(
  drv = RPostgres::Postgres(),
  dbname = "postgres",
  host = "aws-0-eu-west-3.pooler.supabase.com",
  port = 6543,
  user = "postgres.zctxxrgkafjrieubmcuz",
  password = "leACMVQ7MFH9TtFf",
  minSize = 1,  # Minimum number of connections in the pool
  maxSize = 15,  # Maximum number of connections in the pool
  idleTimeout = 600  # Time in seconds to keep an idle connection before closing it
)

# Function to get a connection from the pool
get_pg_connection <- function() {
  con <- poolCheckout(pg_pool)
  return(con)
}

# Function to return the connection to the pool
return_pg_connection <- function(con) {
  poolReturn(con)
}

# Properly close all connections in the pool when done
close_pg_pool <- function() {
  poolClose(pg_pool)
}

# Function to check if a combination exists in the database
combination_exists <- function(con, left_combination, right_combination, target) {
  left_str <- paste(left_combination, collapse = ", ")
  right_str <- paste(right_combination, collapse = ", ")
  
  # Resolve left_id
  left_id_query <- paste("SELECT id FROM left_populations WHERE left_combination = '", left_str, "' LIMIT 1", sep = "")
  left_id_result <- dbGetQuery(con, left_id_query)
  left_id <- left_id_result$id[1]
  
  # Resolve right_id
  right_id_query <- paste("SELECT id FROM right_populations WHERE right_combination = '", right_str, "' LIMIT 1", sep = "")
  right_id_result <- dbGetQuery(con, right_id_query)
  right_id <- right_id_result$id[1]
  
  # Resolve target_id
  target_id_query <- paste("SELECT id FROM target WHERE name = '", target, "' LIMIT 1", sep = "")
  target_id_result <- dbGetQuery(con, target_id_query)
  target_id <- target_id_result$id[1]
  
  # Check if the combination exists in the request table
  query <- paste(
    "SELECT COUNT(*) AS count FROM request WHERE left_id = ", left_id, 
    " AND right_id = ", right_id, 
    " AND target_id = ", target_id, sep = ""
  )
  
  count <- dbGetQuery(con, query)$count[1]
  return(count > 0)
}


# Function to count and log combinations that need computation
count_combinations_needing_computation <- function(combinations) {
  combinations_to_compute <- list()
  
  con_classic <- get_pg_connection_classic()  # Get a connection from the pool
  
  tryCatch({
    for (i in 1:nrow(combinations)) {
      row <- combinations[i, ]
      # Check if the combination already exists
      if (!combination_exists(con_classic, row$left_combination[[1]], row$right_combination[[1]], row$target)) {
        combinations_to_compute <- append(combinations_to_compute, list(row))
      }
    }
    
    log_message(paste(length(combinations_to_compute), "requests needs to be inserted."))
    
  }, error = function(e) {
    log_message(paste("Error while counting combinations needing computation:", e$message))
    
  }, finally = {
    dbDisconnect(con_classic)
  })
  
  return(combinations_to_compute)
}


# Function to determine if in main thread
is_main_thread <- function() {
  Sys.getenv("RSTUDIO_SESSION_PORT") == ""
}

# Custom logging function
log_message <- function(message) {
  main_thread <- if (is_main_thread()) "MainThread" else "WorkerThread"
  workers_count <- future::nbrOfWorkers()
  timestamp <- Sys.time()
  if (is.vector(message) && length(message) > 1) {
    message <- paste(message, collapse = ", ")
  }
  message <- paste("][Time:", timestamp, "]", as.character(message))
  print(paste(message, "\n"))
  write(message, file = progress_log_path, append = TRUE, sep = "\n")
}

# Function to clear a log file
clear_log_file <- function(file_path) {
  write("", file = file_path)
}

# Clear log files
clear_log_file(progress_log_path)
clear_log_file(ongoingCombinationProgress_log_path)
clear_log_file(rejection_log_path)
#clear_log_file(right_populations_path)

# Function to check if required files exist with specific extensions
check_required_files <- function(prefix) {
  extensions <- c(".snp", ".anno", ".geno", ".ind")
  missing_files <- sapply(extensions, function(ext) {
    file_path <- paste0(prefix, ext)
    if (!file.exists(file_path)) {
      return(file_path)
    } else {
      return(NULL)
    }
  })
  
  missing_files <- missing_files[!sapply(missing_files, is.null)]
  
  if (length(missing_files) > 0) {
    stop(paste("Error: The following required files are missing:", paste(missing_files, collapse = ", ")))
  }
}

# Function to ensure a directory exists, create it if not
ensure_dir_exists <- function(file_path) {
  dir_path <- dirname(file_path)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
    log_message(paste("Created directory:", dir_path))
  }
}

# Check if the required prefix_ho files exist
check_required_files(prefix_ho)

# Ensure the directories for the other files exist
ensure_dir_exists(progress_log_path)
ensure_dir_exists(ongoingCombinationProgress_log_path)
ensure_dir_exists(all_weights_data_path)
ensure_dir_exists(p_values_path)
ensure_dir_exists(avg_se_path)
ensure_dir_exists(right_populations_path)
ensure_dir_exists(rejection_log_path)

# If the script reaches this point, all checks have passed
log_message("File and directory checks passed. Proceeding with the script.")

ind_file_path <- paste0(prefix_ho,".ind")

# Read the .ind file
ind_data <- readLines(ind_file_path)

# Extract the third string from each line
ind_labels <- sapply(ind_data, function(line) {
  strsplit(line, "\\s+")[[1]][3]
})

# Convert to a unique set of labels for faster lookup
ind_labels_set <- unique(ind_labels)



# Function to check if labels are present in the .ind file
check_labels_in_ind_file <- function(ind_file_path, left, right, targets) {
  # Read the .ind file
  ind_data <- read.table(ind_file_path, stringsAsFactors = FALSE)
  
  # Extract labels from the .ind file (assuming labels are in the 3rd column)
  ind_labels <- ind_data[, 3]
  
  # Function to check if a label is present in the .ind file
  label_exists <- function(label) {
    if (label == "") {
      return(TRUE)  # Ignore empty labels
    }
    return(label %in% ind_labels)
  }
  
  # Check all left labels
  left_check <- all(sapply(left, label_exists))
  if (!left_check) {
    missing_left <- left[!sapply(left, label_exists)]
    warning("The following left labels are missing from the .ind file: ", paste(missing_left, collapse = ", "))
  }
  
  # Check all right labels
  right_check <- all(sapply(right, label_exists))
  if (!right_check) {
    missing_right <- right[!sapply(right, label_exists)]
    warning("The following right labels are missing from the .ind file: ", paste(missing_right, collapse = ", "))
  }
  
  # Check all target labels
  target_check <- all(sapply(targets, label_exists))
  if (!target_check) {
    missing_targets <- targets[!sapply(targets, label_exists)]
    warning("The following target labels are missing from the .ind file: ", paste(missing_targets, collapse = ", "))
  }
  
  # Return TRUE if all checks pass, otherwise FALSE
  return(left_check && right_check && target_check)
}

left_static <- unlist(strsplit(input_string_left_static, split = ","))
left_dynamic <- unlist(strsplit(input_string_left_dynamic, split = ","))

right_static <- unlist(strsplit(input_string_right_static, split = ","))
right_dynamic <- unlist(strsplit(input_string_right_dynamic, split = ","))

targets <- unlist(strsplit(targets_str, split = ","))

# Combine left and right static and dynamic labels
all_left <- c(left_static, left_dynamic)
all_right <- c(right_static, right_dynamic)

# Call the function to check labels
all_labels_exist <- check_labels_in_ind_file(ind_file_path, all_left, all_right, targets)

if (all_labels_exist) {
  log_message("All labels are present in the .ind file.\n")
} else {
  stop("Some labels are missing from the .ind file. Check the warnings for details.\n")
}


# Check if both left_static and left_dynamic are empty
if (length(left_static) == 0 && length(left_dynamic) == 0) {
  stop("Both left_static and left_dynamic are empty. At least one must be non-empty.")
}

# Check if both right_static and right_dynamic are empty
if (length(right_static) == 0 && length(right_dynamic) == 0) {
  stop("Both right_static and right_dynamic are empty. At least one must be non-empty.")
}





log_message2 <- function(prefix_ho, left, right, target) {
  main_thread <- if (is_main_thread()) "MainThread" else "WorkerThread"
  workers_count <- future::nbrOfWorkers()
  timestamp <- Sys.time()
  if (is.vector(left) && length(left) > 1) {
    left_str <- paste(left, collapse = ", ")
  } else {
    left_str <- left
  }
  if (is.vector(right) && length(right) > 1) {
    right_str <- paste(right, collapse = ", ")
  } else {
    right_str <- right
  }
  message <- paste("][Time:", timestamp, "] process_combination", prefix_ho, left_str, right_str, target, sep = " ")
  print(paste(message, "\n"))
  write(message, file = ongoingCombinationProgress_log_path, append = TRUE, sep = "\n")
}

# Initialize list to store rejection reasons
rejection_reasons <- list()

log_rejection <- function(prefix_ho, left, right, target, reason) {
  main_thread <- if (is_main_thread()) "MainThread" else "WorkerThread"
  workers_count <- future::nbrOfWorkers()
  timestamp <- Sys.time()
  if (is.vector(left) && length(left) > 1) {
    left_str <- paste(left, collapse = ", ")
  } else {
    left_str <- left
  }
  if (is.vector(right) && length(right) > 1) {
    right_str <- paste(right, collapse = ", ")
  } else {
    right_str <- right
  }
  message <- paste("][Time:", timestamp, "]", 
                   "Rejected combination:", prefix_ho, left_str, right_str, target, "Reason:", reason)
  print(paste(message, "\n"))
  write(message, file = rejection_log_path, append = TRUE, sep = "\n")
  
  # Store rejection reason in the list
  rejection_reasons <<- append(rejection_reasons, list(data.frame(prefix_ho = prefix_ho, left = left_str, right = right_str, target = target, reason = reason, stringsAsFactors = FALSE)))
}

# Function to check if a combination has been processed before
check_existing_request <- function(con, left, right, target) {
  # Convert the target name to its ID
  target_id_query <- paste("SELECT id FROM target WHERE name = '", target, "'", sep = "")
  target_id <- dbGetQuery(con, target_id_query)$id[1]
  
  # If the target_id is NULL, return an empty data frame
  if (is.null(target_id)) {
    return(data.frame())
  }
  
  # Construct the SQL query using the left, right IDs, and target_id
  left_str <- paste(left, collapse = ", ")
  right_str <- paste(right, collapse = ", ")
  query <- paste(
    "SELECT * FROM request WHERE left_id = (SELECT id FROM left_populations WHERE left_combination = '", left_str, 
    "') AND right_id = (SELECT id FROM right_populations WHERE right_combination = '", right_str, 
    "') AND target_id = ", target_id, sep = ""
  )
  
  dbGetQuery(con, query)
}

# Function to fetch result for an existing request
fetch_existing_result <- function(con, left, right, target) {
  # Convert the target name to its ID
  target_id_query <- paste("SELECT id FROM target WHERE name = '", target, "'", sep = "")
  target_id <- dbGetQuery(con, target_id_query)$id[1]
  
  # If the target_id is NULL, return an empty data frame
  if (is.null(target_id)) {
    return(data.frame())
  }
  
  # Construct the SQL query using the left, right IDs, and target_id
  left_str <- paste(left, collapse = ", ")
  right_str <- paste(right, collapse = ", ")
  query <- paste(
    "SELECT r.*, lp.left_combination, rp.right_combination, t.name as target_name 
    FROM result r 
    JOIN request req ON r.request_id = req.id 
    JOIN left_populations lp ON req.left_id = lp.id 
    JOIN right_populations rp ON req.right_id = rp.id 
    JOIN target t ON req.target_id = t.id 
    WHERE lp.left_combination = '", left_str, 
    "' AND rp.right_combination = '", right_str, 
    "' AND t.id = ", target_id, sep = ""
  )
  
  dbGetQuery(con, query)
}


# Function to insert and get left ID
insert_and_get_left_id <- function(con, left_combination) {
  left_str <- paste(left_combination, collapse = ", ")
  query <- paste("SELECT id FROM left_populations WHERE left_combination = '", left_str, "' LIMIT 1", sep = "")
  result <- dbGetQuery(con, query)
  
  if (nrow(result) == 0) {
    result <- dbGetQuery(con, paste("INSERT INTO left_populations (left_combination) VALUES ('", left_str, "') RETURNING id", sep = ""))
    if (nrow(result) == 0) {
      stop("Failed to insert and retrieve left_id for combination: ", left_str)
    }
  }
  
  return(result$id[1])
}


# Function to insert and get right ID
insert_and_get_right_id <- function(con, right_combination) {
  right_str <- paste(right_combination, collapse = ", ")
  query <- paste("SELECT id FROM right_populations WHERE right_combination = '", right_str, "' LIMIT 1", sep = "")
  result <- dbGetQuery(con, query)
  
  if (nrow(result) == 0) {
    result <- dbGetQuery(con, paste("INSERT INTO right_populations (right_combination) VALUES ('", right_str, "') RETURNING id", sep = ""))
    if (nrow(result) == 0) {
      stop("Failed to insert and retrieve right_id for combination: ", right_str)
    }
  }
  
  return(result$id[1])
}

insert_and_get_target_id <- function(con, target_name) {
  query <- paste("SELECT id FROM target WHERE name = '", target_name, "' LIMIT 1", sep = "")
  result <- dbGetQuery(con, query)
  
  if (nrow(result) == 0) {
    result <- dbGetQuery(con, paste("INSERT INTO target (name) VALUES ('", target_name, "') RETURNING id", sep = ""))
    if (nrow(result) == 0) {
      stop("Failed to insert and retrieve target_id for target: ", target_name)
    }
  }
  
  return(result$id[1])
}

# Function to convert existing results
convert_existing_results <- function(existing_result, model_id, script_generated_right_id) {
  
  # Extract necessary data from the existing result
  weights_data <- existing_result %>%
    select(population, weight, se, p_value) %>%
    mutate(target = paste0(existing_result$target_name[1], "_mdl", sprintf("%03d", model_id), "_Rght", sprintf("%03d", script_generated_right_id)))
  
  # Prepare result list with rankdrop and popdrop if needed
  result_list <- list(
    weights = weights_data,
    rankdrop = data.frame(),  # This can be populated if required
    popdrop = data.frame(
      pat = sapply(1:nrow(weights_data), function(x) paste0(rep("0", x), collapse = "")),
      p = existing_result$p_value[1]
    )
  )
  
  # Construct the final list to be returned, including necessary combinations
  list(
    result = result_list,
    weights_data = weights_data,  # This includes the mutated target for CSV and plotting
    p_value = existing_result$p_value[1],
    avg_se = mean(weights_data$se),  # Calculate avg_se from the weights data
    right_combination = strsplit(existing_result$right_combination[1], ", ")[[1]],  # Extract the right combination from the first row
    original_target = existing_result$target_name[1],  # Fetch the original target from the first row
    left_combination = strsplit(existing_result$left_combination[1], ", ")[[1]]  # Extract the left combination from the first row
  )
}


# Function to update results_available after computing the results
update_results_available <- function(con, request_id) {
  tryCatch({
    query <- paste("UPDATE request SET results_available = TRUE WHERE id = ", request_id, sep = "")
    dbExecute(con, query)
  }, error = function(e) {
    log_message(paste("Error updating results_available:", e$message))
  })
}

# Define the function to call the stored procedure
callStoredFunction <- function(con, request_id, results_json) {
  query <- paste("SELECT insert_results(", request_id, ", '", results_json, "')", sep = "")
  dbExecute(con, query)
}

process_combination <- function(prefix_ho, left, right, target, model_id, script_generated_right_id, db_right_id) {
  con <- NULL
  query <- NULL
  
  tryCatch({
    con <- get_pg_connection()  # Get a connection from the pool
    
    # Get or insert the target_id
    target_id <- insert_and_get_target_id(con, target)
    
    # Insert or get left_id and right_id from the database
    left_id <- insert_and_get_left_id(con, left)
    
    # Retrieve the request_id (request should already exist)
    query <- paste("SELECT id FROM request WHERE left_id = ", left_id, 
                   " AND right_id = ", db_right_id, 
                   " AND target_id = ", target_id, sep = "")
    request_id <- dbGetQuery(con, query)$id[1]
    
    # Check if results already exist
    query <- paste("SELECT results_available FROM request WHERE id = ", request_id, sep = "")
    result_check <- dbGetQuery(con, query)
    
    if (result_check$results_available[1] == TRUE) {
      log_message("Results already available for this request. Skipping computation.")
      return(NULL)
    }
    
    log_message2(prefix_ho, left, right, target)
    
    # Dedicated tryCatch block for the computation and subsequent processing
    computation_result <- tryCatch({
      # Run the computation
      res <- qpadm(prefix_ho, left, right, target, allsnps = TRUE)
      if (is.null(res)) {
        stop("qpadm returned NULL")
      }
      
      # Generate the mutated target for in-memory operations (e.g., CSV, plotting)
      mutated_target <- paste0(target, "_mdl", sprintf("%03d", model_id), "_Rght", sprintf("%03d", script_generated_right_id))
      
      # Prepare weights_data with the mutated target
      weights_data <- res$weights %>%
        select(left, weight, se) %>%
        rename(population = left) %>%
        mutate(target = mutated_target)
      
      num_left <- length(left)
      current_p_value <- res$popdrop %>%
        filter(pat %in% sapply(1:num_left, function(x) paste0(rep("0", x), collapse = ""))) %>%
        pull(p)
      
      current_avg_se <- mean(weights_data$se)
      
      # Use the stored procedure to insert results
      result_json <- toJSON(weights_data)
      callStoredFunction(con, request_id, result_json)  # Using your stored procedure
      
      # Update results_available in the request table
      update_results_available(con, request_id)
      
      # Return results for in-memory use (e.g., plotting)
      list(
        result = res,
        weights_data = weights_data,  # This includes the mutated target for CSV and plotting
        p_value = current_p_value,
        avg_se = current_avg_se,
        right_combination = right,  # Include right combination in the result for logging
        original_target = target,  # Include the original target
        left_combination = left  # Include the left combination
      )
    }, error = function(e) {
      log_message(paste("Error during computation or subsequent processing for target", target, ":", e$message))
      NULL
    })
    
    return(computation_result)
    
  }, error = function(e) {
    log_message(paste("Error processing combination for target", target, "with left", paste(left, collapse = ", "), "and right", paste(right, collapse = ", "), ":", e$message))
    log_message(paste("Failed SQL query:", query))
    NULL
  }, finally = {
    if (!is.null(con)) {
      return_pg_connection(con)
    }
  })
}





# Function to prepare all combinations for all targets
prepare_combinations <- function(targets, left_static, left_dynamic, right_static, right_dynamic) {
  left_combinations <- unlist(lapply(0:length(left_dynamic), function(n) combn(left_dynamic, n, simplify = FALSE)), recursive = FALSE)
  right_combinations <- unlist(lapply(0:length(right_dynamic), function(n) combn(right_dynamic, n, simplify = FALSE)), recursive = FALSE)
  
  combinations <- expand.grid(target = targets, left_combination = left_combinations, right_combination = right_combinations, stringsAsFactors = FALSE)
  combinations <- combinations %>%
    mutate(left_combination = lapply(left_combination, function(x) c(left_static, x)),
           right_combination = lapply(right_combination, function(x) c(right_static, x)))
  
  # Remove combinations where left_combination or right_combination is empty
  combinations <- combinations %>%
    filter(lengths(left_combination) > 0 & lengths(right_combination) > 0)
  
  combinations
}

# Function to calculate the number of combinations
calculate_combinations_count <- function(dynamic_population) {
  sum(sapply(0:length(dynamic_population), function(k) choose(length(dynamic_population), k)))
}

# Function to add unique constraint if it does not exist
add_unique_constraint_if_not_exists <- function(con, table_name, constraint_name, column_name) {
  # Check if the constraint already exists
  query <- paste(
    "SELECT 1 FROM pg_constraint WHERE conname = '", constraint_name, "' AND conrelid = '",
    table_name, "'::regclass;", sep = ""
  )
  
  result <- dbGetQuery(con, query)
  
  # If the constraint does not exist, add it
  if (nrow(result) == 0) {
    alter_query <- paste(
      "ALTER TABLE ", table_name, " ADD CONSTRAINT ", constraint_name,
      " UNIQUE (", column_name, ");", sep = ""
    )
    dbExecute(con, alter_query)
    cat(paste("Constraint", constraint_name, "added to", table_name, "\n"))
  } else {
    cat(paste("Constraint", constraint_name, "already exists on", table_name, "\n"))
  }
}

# Function to insert all distinct left combinations into the database
insert_all_left_combinations <- function(con, left_combinations) {
  for (left_combination in left_combinations) {
    left_str <- paste(left_combination, collapse = ", ")
    query <- paste("SELECT id FROM left_populations WHERE left_combination = '", left_str, "' LIMIT 1", sep = "")
    result <- dbGetQuery(con, query)
    
    if (nrow(result) == 0) {
      dbExecute(con, paste("INSERT INTO left_populations (left_combination) VALUES ('", left_str, "')", sep = ""))
    }
  }
}

# Function to insert all distinct right combinations into the database
insert_all_right_combinations <- function(con, right_combinations) {
  for (right_combination in right_combinations) {
    right_str <- paste(right_combination, collapse = ", ")
    query <- paste("SELECT id FROM right_populations WHERE right_combination = '", right_str, "' LIMIT 1", sep = "")
    result <- dbGetQuery(con, query)
    
    if (nrow(result) == 0) {
      dbExecute(con, paste("INSERT INTO right_populations (right_combination) VALUES ('", right_str, "')", sep = ""))
    }
  }
}

# Function to insert all targets into the target table if they don't already exist
insert_all_targets <- function(con, targets) {
  for (target in targets) {
    query <- paste("INSERT INTO target (name) VALUES ('", target, "') ON CONFLICT (name) DO NOTHING", sep = "")
    dbExecute(con, query)
  }
  log_message("All targets have been inserted into the target table.")
}

# Obtain a connection from the connection pool
con_classic <- get_pg_connection_classic()

tryCatch({
  # Create the target table if it doesn't exist
  dbExecute(con_classic, "
        CREATE TABLE IF NOT EXISTS target (
            id SERIAL PRIMARY KEY, 
            name TEXT UNIQUE
        );
    ")
  
  # Create the left_populations table if it doesn't exist
  dbExecute(con_classic, "
        CREATE TABLE IF NOT EXISTS left_populations (
            id SERIAL PRIMARY KEY, 
            left_combination TEXT UNIQUE
        );
    ")
  
  # Create the right_populations table if it doesn't exist
  dbExecute(con_classic, "
        CREATE TABLE IF NOT EXISTS right_populations (
            id SERIAL PRIMARY KEY, 
            right_combination TEXT UNIQUE
        );
    ")
  
  # Create the request table if it doesn't exist, including a results_available column
  dbExecute(con_classic, "
    CREATE TABLE IF NOT EXISTS request (
        id SERIAL PRIMARY KEY, 
        left_id INTEGER, 
        right_id INTEGER, 
        target_id INTEGER, 
        results_available BOOLEAN DEFAULT FALSE,  -- Add this line to track if results are available
        FOREIGN KEY(left_id) REFERENCES left_populations(id), 
        FOREIGN KEY(right_id) REFERENCES right_populations(id),
        FOREIGN KEY(target_id) REFERENCES target(id),
        UNIQUE(left_id, right_id, target_id)
    );
")
  
  # Create the result table if it doesn't exist, including p_value
  dbExecute(con_classic, "
        CREATE TABLE IF NOT EXISTS result (
            id SERIAL PRIMARY KEY, 
            request_id INTEGER, 
            population TEXT, 
            weight DOUBLE PRECISION, 
            se DOUBLE PRECISION, 
            p_value DOUBLE PRECISION,
            FOREIGN KEY(request_id) REFERENCES request(id),
            UNIQUE(request_id, population, weight, se, p_value)
        );
    ")
  
  # Add unique constraints to the left_populations and right_populations tables
  add_unique_constraint_if_not_exists(con_classic, "left_populations", "unique_left_combination", "left_combination")
  add_unique_constraint_if_not_exists(con_classic, "right_populations", "unique_right_combination", "right_combination")
  
  # Add a unique constraint to the request table to ensure no duplicate combinations of left_id, right_id, and target_id
  add_unique_constraint_if_not_exists(con_classic, "request", "unique_request_combination", 
                                      "left_id, right_id, target_id")
  
  # Add a unique constraint to the result table to prevent duplicates
  add_unique_constraint_if_not_exists(con_classic, "result", "unique_result_combination", 
                                      "request_id, population, weight, se, p_value")
  
  # Add the unique constraint for request_id and population specifically
  add_unique_constraint_if_not_exists(con_classic, "result", "unique_request_population", 
                                      "request_id, population")
  
  
}, error = function(e) {
  log_message(paste("Error during database setup:", e$message))
}, finally = {
  if (!is.null(con_classic)) {
    dbDisconnect(con_classic)  # Return the connection to the pool
  }
})


# Record start time
start_time <- Sys.time()
log_message(paste("Start time:", start_time))



# Calculate the number of combinations for one target
num_combinations_per_target <- calculate_combinations_count(left_dynamic) * calculate_combinations_count(right_dynamic)
log_message(paste("There will be", num_combinations_per_target, "combinations for one target."))

# Calculate the total number of combinations for all targets
total_combinations <- num_combinations_per_target * length(targets)
log_message(paste("There will be a total of", total_combinations, "combinations for all targets."))

# Prepare all combinations
combinations <- prepare_combinations(targets, left_static, left_dynamic, right_static, right_dynamic)

# Extract all distinct left and right combinations
distinct_left_combinations <- unique(combinations$left_combination)
distinct_right_combinations <- unique(combinations$right_combination)

# Insert all distinct left and right combinations into the database using connection pooling
con_classic <- get_pg_connection_classic()  # Obtain a connection from the pool

tryCatch({
  insert_all_left_combinations(con_classic, distinct_left_combinations)
  insert_all_right_combinations(con_classic, distinct_right_combinations)
  insert_all_targets(con_classic, targets)
}, error = function(e) {
  log_message(paste("Error during insertion of combinations:", e$message))
}, finally = {
  if (!is.null(con_classic)) {
    dbDisconnect(con_classic)  # Return the connection to the pool
  }
})


log_message("All distinct left and right and target combinations have been inserted into the database.")

# Count and log combinations needing computation
combinations_to_compute <- count_combinations_needing_computation(combinations)

# Insert all distinct left and right combinations into the database using connection pooling
con_classic <- get_pg_connection_classic()  # Obtain a connection from the pool

# Function to insert all requests before computation
insert_requests <- function(con, combinations) {
  for (i in 1:nrow(combinations)) {
    row <- combinations[i, ]
    
    # Convert combinations to string
    left_str <- paste(row$left_combination[[1]], collapse = ", ")
    right_str <- paste(row$right_combination[[1]], collapse = ", ")
    
    # Get the corresponding IDs
    left_id <- insert_and_get_left_id(con, left_str)
    right_id <- insert_and_get_right_id(con, right_str)
    target_id <- insert_and_get_target_id(con, row$target)
    
    # Insert the request with results_available set to FALSE
    query <- paste("INSERT INTO request (left_id, right_id, target_id, results_available) VALUES (", 
                   left_id, ",", right_id, ",", target_id, ", FALSE) ON CONFLICT DO NOTHING", sep = "")
    dbExecute(con, query)
  }
  log_message("All requests have been inserted into the request table.")
}

# Insert all requests
tryCatch({
  insert_requests(con_classic, combinations)
}, error = function(e) {
  log_message(paste("Error during insertion of requests:", e$message))
}, finally = {
  if (!is.null(con_classic)) {
    dbDisconnect(con_classic)
  }
})

# Generate script-generated right combination IDs
right_combination_ids <- combinations %>%
  select(right_combination) %>%
  distinct() %>%
  mutate(script_generated_right_id = row_number())

# Function to get or insert right_id based on right_combination in the database
get_or_insert_db_right_id <- function(con, right_combination) {
  right_str <- paste(right_combination, collapse = ", ")
  query <- paste("SELECT id FROM right_populations WHERE right_combination = '", right_str, "' LIMIT 1", sep = "")
  result <- dbGetQuery(con, query)
  
  if (nrow(result) == 0) {
    result <- dbGetQuery(con, paste("INSERT INTO right_populations (right_combination) VALUES ('", right_str, "') RETURNING id", sep = ""))
  }
  
  return(result$id[1])
}

# Generate database right IDs by fetching from the database
con_classic <- get_pg_connection_classic()

tryCatch({
  right_combination_ids <- right_combination_ids %>%
    rowwise() %>%
    mutate(db_right_id = get_or_insert_db_right_id(con_classic, right_combination))
}, error = function(e) {
  log_message(paste("Error during database right ID generation:", e$message))
}, finally = {
  if (!is.null(con_classic)) {
    dbDisconnect(con_classic)  # Return the connection to the pool
  }
})

# Ungroup the data after rowwise operation
right_combination_ids <- right_combination_ids %>% ungroup()


# Join right combination IDs back to combinations
combinations <- combinations %>%
  left_join(right_combination_ids, by = "right_combination") %>%
  mutate(model_id = row_number())

# Save all right combinations with IDs
right_combinations_df <- right_combination_ids %>%
  rowwise() %>%
  mutate(right_combination = paste(unlist(right_combination), collapse = ", ")) %>%
  ungroup() %>%
  rename(RightId = script_generated_right_id, Right = right_combination)


# Function to safely disconnect from the database
safe_disconnect <- function(con) {
  if (!is.null(con) && dbIsValid(con)) {
    dbDisconnect(con)
    cat("Disconnected from the database.\n")
  }
}

# Check for existing results and fetch them from the database if they exist
con_classic <- get_pg_connection_classic()

tryCatch({
  existing_results <- list()
  new_combinations <- list()
  
  for (i in 1:nrow(combinations)) {
    row <- combinations[i, ]
    target <- row$target
    left <- row$left_combination[[1]]
    right <- row$right_combination[[1]]
    
    script_generated_right_id <- row$script_generated_right_id
    db_right_id <- row$db_right_id
    
    tryCatch({
      # Check if the request exists in the database using the db_right_id
      existing_request <- check_existing_request(con_classic, left, right, target)
      
      # If an existing request is found, check for the corresponding result
      if (nrow(existing_request) > 0) {
        existing_result <- fetch_existing_result(con_classic, left, right, target)
        if (nrow(existing_result) > 0) {
          existing_results <- append(existing_results, list(list(result = existing_result, model_id = row$model_id, right_id = script_generated_right_id)))
        } else {
          new_combinations <- append(new_combinations, list(row))
        }
      } else {
        new_combinations <- append(new_combinations, list(row))
      }
    }, error = function(e) {
      log_message(paste("Error during combination check for target:", target, "-", e$message))
    })
  }
  
  log_message(paste("Total combinations:", nrow(combinations)))
  log_message(paste("Total new_combinations needing computation:", length(new_combinations)))
  log_message(paste("Total existing_results:", length(existing_results)))
  
}, error = function(e) {
  log_message(paste("Error during main loop:", e$message))
}, finally = {
  if (!is.null(con_classic)) {
    dbDisconnect(con_classic)  # Return the connection to the pool
  }
})


# Setup parallel backend
plan(multisession, workers = parallel::detectCores() - 1)


# Initialize a list to capture failed combinations
failed_combinations <- list()

# Process new combinations in parallel
results <- future_map(new_combinations, function(row) {
  target <- row$target
  left <- row$left_combination[[1]]
  right <- row$right_combination[[1]]
  model_id <- row$model_id
  script_generated_right_id <- row$script_generated_right_id  # Right ID generated by the script for in-memory use
  db_right_id <- row$db_right_id  # Right ID from the database for database operations
  
  res <- tryCatch(
    {
      # Pass both the script-generated and database right IDs to process_combination
      process_combination(prefix_ho, left, right, target, model_id, script_generated_right_id, db_right_id)
    },
    error = function(e) {
      log_message(paste("Error processing combination for target", target, "with left", paste(left, collapse = ", "), "and right", paste(right, collapse = ", "), ":", e$message))
      
      # Capture the failed combination for retry
      failed_combinations <<- append(failed_combinations, list(row))
      
      NULL
    }
  )
  
  if (is.null(res)) {
    log_message(paste("Combination for target", target, "with left", paste(left, collapse = ", "), "and right", paste(right, collapse = ", "), "returned NULL"))
  } else {
    log_message(paste("Combination for target", target, "with left", paste(left, collapse = ", "), "and right", paste(right, collapse = ", "), "returned a result"))
  }
  
  res
}, .options = furrr_options(seed = TRUE, scheduling = 1))

# Retry the failed combinations once
if (length(failed_combinations) > 0) {
  log_message("Retrying failed combinations...")
  
  retry_results <- future_map(failed_combinations, function(row) {
    target <- row$target
    left <- row$left_combination[[1]]
    right <- row$right_combination[[1]]
    model_id <- row$model_id
    script_generated_right_id <- row$script_generated_right_id
    db_right_id <- row$db_right_id
    
    res <- tryCatch(
      {
        process_combination(prefix_ho, left, right, target, model_id, script_generated_right_id, db_right_id)
      },
      error = function(e) {
        log_message(paste("Error processing combination on retry for target", target, "with left", paste(left, collapse = ", "), "and right", paste(right, collapse = ", "), ":", e$message))
        NULL
      }
    )
    
    if (is.null(res)) {
      log_message(paste("Retry for combination for target", target, "with left", paste(left, collapse = ", "), "and right", paste(right, collapse = ", "), "returned NULL"))
    } else {
      log_message(paste("Retry for combination for target", target, "with left", paste(left, collapse = ", "), "and right", paste(right, collapse = ", "), "returned a result"))
    }
    
    res
  }, .options = furrr_options(seed = TRUE, scheduling = 1))
  
  # Combine initial results and retry results
  results <- c(results, retry_results)
}


# Convert existing results to the same format as new results
existing_results_converted <- lapply(existing_results, function(x) {
  convert_existing_results(x$result, x$model_id, x$right_id)
})


# Combine results and existing results
results <- c(results, existing_results_converted)

# Filter out NULL results
valid_results <- Filter(Negate(is.null), results)

# Separate valid and invalid results
valid_results_filtered <- list()
best_invalid_results <- list()

for (target in targets) {
  target_results <- Filter(function(res) grepl(target, res$weights_data$target[1]), valid_results)
  
  if (length(target_results) > 0) {
    valid_target_results <- Filter(function(res) {
      if (all(res$weights_data$weight >= 0) && res$p_value >= 0.05 && res$avg_se <= 0.05) {
        TRUE
      } else {
        log_rejection(prefix_ho, res$weights_data$population, res$right_combination, target, paste("Invalid model - P : ",res$p_value," SE : ",res$avg_se))
        FALSE
      }
    }, target_results)
    
    if (length(valid_target_results) > 0) {
      valid_results_filtered[[target]] <- valid_target_results
    } else {
      sorted_results <- target_results[order(sapply(target_results, function(res) -res$p_value),
                                             sapply(target_results, function(res) res$avg_se))]
      
      best_invalid_result <- Filter(function(res) all(res$weights_data$weight >= 0), sorted_results)
      if (length(best_invalid_result) > 0) {
        best_invalid_results[[target]] <- best_invalid_result[[1]]
      } else {
        best_invalid_results[[target]] <- sorted_results[[1]]
      }
    }
  } else {
    log_message(paste("No results found for target:", target))
  }
}

# Ensure each target has at least one result
final_results <- list()
for (target in targets) {
  if (!is.null(valid_results_filtered[[target]]) && length(valid_results_filtered[[target]]) > 0) {
    final_results <- append(final_results, valid_results_filtered[[target]])
  } else if (!is.null(best_invalid_results[[target]])) {
    final_results <- append(final_results, list(best_invalid_results[[target]]))
  }
}

# Combine results for each target
all_weights_data <- bind_rows(lapply(final_results, `[[`, "weights_data"))

# Flatten the p_value and avg_se dictionaries to single values
p_values_dict <- setNames(sapply(final_results, function(res) res$p_value), sapply(final_results, function(res) res$weights_data$target[1]))
avg_se_dict <- setNames(sapply(final_results, function(res) res$avg_se), sapply(final_results, function(res) res$weights_data$target[1]))

# Convert dictionaries to dataframes
p_values_df <- enframe(p_values_dict, name = "target", value = "p_value")
avg_se_df <- enframe(avg_se_dict, name = "target", value = "avg_se")

check_and_rename_file <- function(file_path) {
  if (file.exists(file_path)) {
    timestamp <- format(Sys.time(), "%d%m%y%H%M")
    file_ext <- tools::file_ext(file_path)
    file_name <- sub(paste0("\\.", file_ext, "$"), "", basename(file_path))
    new_file_path <- file.path(dirname(file_path), paste0(file_name, "_", timestamp, ".", file_ext))
    file.rename(file_path, new_file_path)
  }
}

# Check and rename existing files
check_and_rename_file(all_weights_data_path)
check_and_rename_file(p_values_path)
check_and_rename_file(avg_se_path)
check_and_rename_file(right_populations_path)

# Save the data to a file
write_csv(all_weights_data, all_weights_data_path)
write_csv(p_values_df, p_values_path)
write_csv(avg_se_df, avg_se_path)
write_csv(right_combinations_df, right_populations_path)

# Save the rejection reasons to a CSV file
if (length(rejection_reasons) > 0) {
  rejection_df <- bind_rows(rejection_reasons)
  write_csv(rejection_df, rejection_log_path)
}

# Verify saved data
all_weights_data_check <- read_csv(all_weights_data_path)
p_values_check <- read_csv(p_values_path)
avg_se_check <- read_csv(avg_se_path)
right_populations_check <- read_csv(right_populations_path)

# Debugging: Print to verify saved data
log_message("all_weights_data_check:")
log_message(capture.output(print(all_weights_data_check)))
log_message("p_values_check:")
log_message(capture.output(print(p_values_check)))
log_message("avg_se_check:")
log_message(capture.output(print(avg_se_check)))
log_message("right_populations_check:")
log_message(capture.output(print(right_populations_check)))

close_pg_pool()

# Record end time
end_time <- Sys.time()
log_message(paste("End time:", end_time, "\n"))
script_duration_seconds <- as.numeric(difftime(end_time, start_time, units = "secs"))
script_duration_formatted <- sprintf("%02d:%02d:%02d", as.integer(script_duration_seconds %/% 3600), as.integer((script_duration_seconds %% 3600) %/% 60), as.integer(script_duration_seconds %% 60))
log_message(paste("Script duration:", script_duration_formatted))
