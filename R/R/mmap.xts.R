#!/usr/bin/Rscript
# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# "Author: Nathan Matare <nathan.matare@chicagobooth.edu>"
#
#' @title 
#' 'as.mmap.xts'
#' 
#' @description 
#' mmap method for xts objects
#' 
#' 
#' @export 
#' @TODO convert this to s3 method for mmap objects
as.mmap.xts <- function(x, file = tempfile()){

    bytes <- sizeof(as.Ctype(double())) * (ncol(x) + 1) * nrow(x) 
    base::writeBin(rep(as.raw(0), bytes), file)

    structure <- do.call(struct,  
        c(index = as.double(0), 
        sapply(x, function(i) 
            (do.call(storage.mode(i), list(0)))
    )))

    map <- xts::mmap(
        file        = file, 
        mode        = structure,
        extractFUN  = function(x){
            xts(x           = do.call(cbind, x[-1L]), 
                order.by    = as.POSIXct(
                    x       = x[[1L]], 
                    tz      = "America/New_York",
                    trim    = "%Y-%m-%d %H:%M:%S",
                    origin  = '1970-01-01')
    )})

    # now place data
    map[ ,1L] <- .index(x) 
    for(i in seq(ncol(x))) 
        map[ ,i + 1L] <- coredata(x)[ ,i]

    # make sure bytes were calc'ed correctly 
    stopifnot(all.equal(dim(x), dim(map[])))  
    return(map)
}