library(Rhipe)
rhinit()
rhoptions(runner=sprintf("%s/rhipe.runner.sh",Sys.getenv("HOME")))




################################################################################################
# Start by creating assignment keys on the hdfs
# A second pass will do the simulation given the keys
################################################################################################


a = list()

ofolder = "/wsc/jrounds/marginal/convolution/simulation/assignments"
inout = c("lapply", "sequence")
N = 1


shared = c("/wsc/jrounds/marginal/Rdata/marginals.Rdata")
setup = expression({
	load("marginals.Rdata")
	key.levels = attr(marginals,"key.levels")
	NCONVOLUTIONS = 40
	key.levels$convolution = 2:NCONVOLUTIONS
	sim.keys = expand.grid(key.levels)
 
})
map = expression({
	NSIMULATIONS = 10*100
	key = data.frame(run = 1)
	for(i in 1:NSIMULATIONS){
		key$run = i
		rhcollect(key, TRUE)
	}

})
reduce = expression(
	pre = {
	},
	reduce = {
	
	},
	post = {
		key = cbind(reduce.key, sim.keys)
		for(i in 1:nrow(key)){
			k = key[i,]
			new.key = k
			new.key$set.seed = k$convolution + 10^2*k$utilization + 10^4*k$rate + 10^6*k$run
			rhcollect(new.key, TRUE)
		}
	}
)
jobname = "Simulation Assignments"
mr = rhmr(map = map, reduce = reduce, setup=setup, inout=inout,ofolder=ofolder, N=N, shared=shared, jobname = jobname)
ex = rhex(mr)

odata = rhread(ofolder, max=100000)
set.seed = unlist(lapply(odata,function(d) d[[1]]$set.seed))
length(odata)
length(unique(set.seed))
odata[[1]]

################################################################################################
# Now we will do the actual simulation.  For each simulation we draw 10k samples from the convolution
# again we round to the 4th digit.
################################################################################################
ifolder = "/wsc/jrounds/marginal/convolution/simulation/assignments"
ofolder = "/wsc/jrounds/marginal/convolution/simulation/sim1"
inout = c("sequence","sequence")

shared = c("/wsc/jrounds/marginal/Rdata/marginals.Rdata")
setup = expression({
	load("marginals.Rdata")
	marginal.keys = attr(marginals,"keys")
	get.marginal = function(rate, utilization){
		which = which(marginal.keys$rate == rate & marginal.keys$utilization == utilization)
		return(marginals[[which]])
	}
 
})

map = expression({
	NDRAWS = 10000
	for(k in map.keys){
		set.seed(k$set.seed)
		marginal = get.marginal(k$rate, k$utilization)
		conv = k$convolution
		sample = sample(marginal$jitter, conv*NDRAWS,replace=TRUE, prob=marginal$density)
		sample = matrix(sample, NDRAWS, conv)
		result = apply(sample, 1, sum)
		result = round(result,4)
		rhcollect(k, result)
		
		
	}
})

jobname = "Simulation 1"
mr = rhmr(map = map, setup=setup, inout=inout,ifolder = ifolder, ofolder=ofolder, shared=shared, jobname = jobname)
ex = rhex(mr, async=TRUE)

odata = rhread(ofolder, max=10)

