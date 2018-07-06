#' @title 
#' 'Quickly Deployable Project (qdp)'
#'
#' @description 
#' Creates a standardized scaffolding to quickly deploy an analysis(project)
#' 
#' @details 
#' 'qdp' creates a scaffolding to enable an analyst to do 
#' what an analyst does best: analysis
#' 
#' Once a 'project' is created using 'create_project', the analyst may load
#' accompanying scripts, configuration files, data, reports, etc by running
#' 'start_project'
#' 
#' The project's directory structure is specified as such:
#' project              # name of project
#'  │ 
#'  ├── README.md       # readme
#'  ├── .git/           # git repo 
#'  ├── inst/           # installation
#'  ├── data/           # data
#'  ├── lib/            # library code
#'  │   ├── class.py    # source code (e.g., .R, .py files)
#'  │   └── analysis.R  # analysis script
#'  ├── conf/
#'  │    ├── config.py  # python config files
#'  │    └── config.R   # R config files
#'  ├── lit/            # literature
#'  ├── report/         # reports
#'  └── log/            # log files
#' 
#' Optional arguments to override defaults: 
#' 
#' 'r_config_options'       .Rprofile-like options; however, project specific
#' 
#' 'py_config_options'      creates config.py which may imported as 
#'                          'import config' for python
#' 
#' 'spark_config_options'   creates the default configuration for Spark
#' 
#' 
#' 'module_setup_options'   other R scripts to source upon setup; 
#'                          e.g., source('project/lib/analysis.R')
#' 
#' 'git_config_options'     git configuration options upon initialization
#' 
#' 'git_ignore_options'     git ignore options upon initialization
#' 
#' The project's default path is set to: 
#' 
#' Linux:       /home/user/projects
#' Windows:     C:/Users/user/projects
#' Mac:         /Users/user/projects
#' 
#' @param project The name of the project
#' 
#' @param path Path to project; default is home/user/project/the_name_of_project
#'
#' @param cores  The number of cores to initialize. Default is NULL which 
#' uses all available cores
#' 
#' @author      Nathan Matare <email: nmatare@chicagobooth.com>
#'
#' @export
#' 
init_project <- start_project <- function(project, path="", cores=NULL, 
  modules=NULL, python=3, ...)
{
	os <- Sys.info()[["sysname"]]
  username <- Sys.info()[["user"]]

  python <- switch(match.arg(as.character(python), c("2", "3")),
    "2"="python2",
    "3"="python3"
  )

  if(is.null(path) || path == "")
    root_dir <- switch(os,
        "Linux"   =
            file.path("/home", username, "projects", project),
        "Darwin"  =
            file.path("Users", username, "projects", project),
        "Windows" =
            file.path("C:Users", username, 
                      "Documents", "projects", project)
    )
  else
    root_dir <- path

  root_dir <<- normalizePath(root_dir, winslash="/", mustWork=FALSE)
  
  if(is.null(cores)) # system("grep -c ^processor /proc/cpuinfo")
    .cores <<- parallel::detectCores()-1L
  else
    if(cores > parallel::detectCores())
        stop("You have specified more cores than are available")
    else
        .cores <<- cores

  if(!dir.exists(path=root_dir)){
    cat(paste0("... creating new project ", project))
    
    dir.create(path=root_dir) # create parent 
    branch_dirs <- file.path(root_dir, 
        c("inst", "data", "lib", "conf", "lit", "report", "log"))
    invisible(sapply(branch_dirs, dir.create, showWarnings=FALSE))

    # Project Configuration
    # R Options
    if(!hasArg(r_config_options))
      r_config_options <- paste(
        "# R Convenience Options",
        "options(width=200)",
        "options(digits.secs=6)",
        "options(scipen=999)",
        "options(digits=6)",
        "\r",
        "# Reticulate/rPython Options",
        "require(reticulate)",
        paste0("Sys.setenv(RETICULATE_PYTHON='/usr/bin/", python, "')"),
        "\r",
        "# Timezone Options",
        "Sys.setenv(TZ='America/New_York')",
        "\r",
        "# Parallel Options",
        "require(doMC, quietly=TRUE)",
        "require(data.table, quietly=TRUE)",
        "data.table::setDTthreads(.cores)",
        "doMC::registerDoMC(.cores)"
        sep = "\n"
      )

      usethis:::write_union(root_dir, quiet=TRUE,
          file.path("conf", "config.R"), r_config_options)

      # Python Options
      if(!hasArg(py_config_options))
          py_config_options <- paste(
              "# Pandas Options",
              "import pandas as pd",
              "pd.set_option('display.float_format', lambda x: '%.3f' % x)", 
              "\r",
              "# Spark Options",
              "import os",
              paste0("os.environ['PYSPARK_PYTHON']='", python, "'"), sep="\n"
          )

      usethis:::write_union(root_dir, quiet=TRUE,
          file.path("conf", "config.py"), py_config_options)

      # Spark Options
      if(!hasArg(spark_config_options))
          spark_config_options <- c(
            "custom: ",
            " # local-only configuration",
            " spark.env.SPARK_LOCAL_IP.local: 127.0.0.1",
            " \r",
            " # include the embedded csv package for spark 1.x",
            " sparklyr.csv.embedded: '^1.*'",
            " \r",
            " # include the databricks avro extension",
            " spark.jars.packages:    com.databricks:spark-avro_2.11:4.0.0"
          )

      usethis:::write_union(root_dir, quiet=TRUE,
          file.path("conf", "spark-custom.yml"), spark_config_options)

      # Library Options
      if(!hasArg(module_setup_options))
        module_setup_options <- c(
        "# Init Setup",
        "if(!library(putils, logical.return=TRUE))", 
        "  devtools::install_github('nmatare/putils', subdir='/R', reload=TRUE)"
        )        

      usethis:::write_union(root_dir, quiet=TRUE,
          file.path("lib", "analysis.R"), module_setup_options)

      # Git Options
      project_repo <- git2r::init(path=root_dir, bare=FALSE)
      genesis_commit <- 
      "In the beginning was the Word, and the Word was 'Arrrgh! 
          ~Piracticus 13:7"

      if(hasArg(git_config_options))
          git2r::config(remote.origin=git_config_options$remote.origin, ...)

      if(!hasArg(git_ignore_options))
          git_ignore_options <- c(
              file.path(root_dir, "data", "*"), 
              ".rdata", ".csv", ".fh", ".feather",
              ".rds", ".rda", ".tar", ".avro", ".csv.gz", "tar.gz")

      usethis:::write_union(root_dir, quiet=TRUE,
          file.path(".git", ".gitignore"), git_ignore_options)

      git2r::add(project_repo, ".")
      git2r::commit(project_repo, genesis_commit)
  }

  suppressWarnings(suppressMessages({ # Source configuration files
      source(file.path(root_dir, "conf", "config.R"))
      if(!is.null(modules))           # Source scripts(modules)
        sapply(modules, function(x) source(file.path(root_dir, "lib", x)))  
  }))

  setwd(root_dir)
  cat(paste("\n", "... initialized", project, "with", 
      .cores, "cores", "\n"), ...)
}

#' @param package A character vector specifying the name of the package 
#' @export
install_cran_package <- function(package){
    if(!library(package, logical.return=TRUE, character.only=TRUE, 
        quietly=TRUE, warn.conflicts=FALSE))
        install.packages(
            pkgs=package, 
            repos='http://cran.us.r-project.org', 
            dependencies=TRUE)
        if(!require(package, character.only=TRUE, quietly=TRUE))
            stop(paste("Could not install", package))
    else 
        invisible(TRUE)
} 

#' @param package A character vector specifying the github directory
#' @export
install_github_package <- function(github_directory){
    package <- gsub(".*/", "" , github_directory)
    if(!library(package, logical.return=TRUE, character.only=TRUE, 
        quietly=TRUE, warn.conflicts=FALSE))
        devtools::install_github(github_directory)
        if(!require(package, character.only=TRUE))
            stop(paste("Could not install", package))
    invisible(TRUE)
}
