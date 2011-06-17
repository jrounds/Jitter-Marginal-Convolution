library(Rhipe)
rhinit()
rhoptions(runner=sprintf("%s/rhipe.runner.sh",Sys.getenv("HOME")))


a = list()
a$ifolder = "/wsc/jxia/queueing/simulation/R.database/SEM/jitter/"
a$ofolder = "/wsc/jrounds/queueing/marginal/counts"
a$inout = c("map","sequence")







itest = rhread(a$ifolder, type="map", max=2)

################################################################################################
# Test manipulations
################################################################################################
k = itest[[1]][[1]]
v = itest[[1]][[2]]
head(v)

round(head(v),5)

names = names(v)
c = 15
name = names[c]
name = sub("jitter","",name)
name = as.numeric(name)


v = itest[[1]][[2]]
jitter = round(v[,15], 4)
t1 = table(jitter)
head(t1)

v = itest[[2]][[2]]
jitter = round(v[,15], 4)
t2 = table(jitter)

head(t2)
################################################################################################
# The goal of this job is to create counts of each column jitter1...jitter9
################################################################################################

a$map = expression({
	for(i in seq_along(map.values)){
		k = map.keys[[i]]
		v = map.values[[i]]
		rate = k[1]
		run = k[2]
			
		new.key = data.frame(rate = rate)
		names = names(v)
		cols = grep("jitter", names)
		for(c in cols){
			jitter = round(v[,c],4)
			name = names[c]
			name = sub("jitter","",name)
			new.key$utilization = as.numeric(name)
			t = table(jitter)
			jitter = as.numeric(names(t))
			for(j in seq_along(t)){
				new.key$jitter = jitter[j]
				new.value = t[j]  #counts
				rhcollect(new.key, new.value)
			}
		}
	}
})
a$reduce = expression(
	pre = {
		count = 0
	},
	reduce = {
		count = count + sum(unlist(reduce.values))
	},
	post = {
		new.value = data.frame(jitter = reduce.key$jitter, count = count)
		new.value = as.matrix(new.value)
		reduce.key$jitter = NULL
		rhcollect(reduce.key, new.value)
	}	
)
a$jobname = "Count Jitter Values"

mr = do.call("rhmr",a)
ex = rhex(mr, async=TRUE)


odata = rhread(a$ofolder, max=10)
odata


################################################################################################
# Looks good now collecting it up into matrix/data.frame
################################################################################################



a = list()
a$ifolder = "/wsc/jrounds/queueing/marginal/counts"
a$ofolder = "/wsc/jrounds/queueing/marginal/collected.counts"
a$inout = c("sequence","sequence")


a$map = expression({
	for(i in seq_along(map.values)){
		rhcollect(map.keys[[i]],map.values[[i]])
	}

})

a$reduce = expression(
	pre = {
		data = list()
	},
	reduce = {
		data = append(data, reduce.values)
	},
	post = {
		data = do.call("rbind", data)
		data = as.matrix(data)
		order = order(data[,"jitter"])
		data = data[order,]
		rhcollect(reduce.key,data)
		
	}
)

a$jobname = "Collect Counts"
mr = do.call("rhmr",a)

ex = rhex(mr, async=TRUE)
			
counts = rhread(a$ofolder)
		
			
			
################################################################################################
# got counts from the server
################################################################################################
			
load("marginal.counts.Rdata")
keys = lapply(counts, function(d) d[[1]])
values = lapply(counts, function(d) as.data.frame(d[[2]]))
max = lapply(values, function(v) v$jitter[which.max(v$count)])
#all 0 which is to be expected
npackets = lapply(values, function(v) sum(v$count))
zero.count = lapply(values, function(v) max(v$count))
values = lapply(values, function(v) v[-which.max(v$count),])  #remove zero
max = lapply(values, function(v) v$jitter[which.max(v$count)])


#add density column
for(i in seq_along(values)) values[[i]]$density = values[[i]]$count/sum(values[[i]]$count)
for(v in values) print(summary(v))

#sort these by rate
rate = unlist(lapply(keys,function(k) k$rate))
utilization = unlist(lapply(keys, function(k) k$utilization))
order = order(rate,utilization)
keys = keys[order]
values = values[order]

postscript("marginal.ps")
for(i in seq_along(keys)){
	k = keys[[i]]
	v = values[[i]]
	main = paste("rate", k$rate, "utilization", k$utilization)
	plot(v$jitter,v$density, type="l",main=main,xlab="jitter", ylab="density")
}
dev.off()

