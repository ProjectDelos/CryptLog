pdf('rplot.pdf')

# plot s.t.
# Data series are in col 1
# X is in col 2
# Y is in the last col
my_plot <- function(d, title, xlab, ylab){
  print(d)
  xs <- unique(d[,2], incomparables = FALSE) 
  print(xs)
  s1 <- subset(d, mode == 0, select=c(t_per_op))
  s2 <- subset(d, mode == 1, select=c(t_per_op))
  s3 <- subset(d, mode == 2, select=c(t_per_op))
  s4 <- subset(d, mode == 4, select=c(t_per_op))
  print(s1)
  print(s2)
  ymax <- max(c(s1[,1], s2[,1]))
  print(ymax)
  plot(xs, s1[,1], col='green', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, ymax), xlab=xlab, ylab=ylab, main=title)
  par(new=T)
  plot(xs, s2[,1], col='blue' , type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, ymax), xlab='', ylab='', axes=F)
  par(new=T)
  plot(xs, s3[,1], col='red' , type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, ymax), xlab='', ylab='', axes=F)
  par(new=T)
  plot(xs, s4[,1], col='purple' , type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, ymax), xlab='', ylab='', axes=F)
  legend(0, ymax, 
         c("-VM-ENC", "-VM+ENC", "+VM-ENC", "+VM+ENC"),
         text.width = max(xs)*0.20,
         lty=c(1,1,1,1),
         lwd=c(2.5,2.5,2.5,2.5), col=c("green", "blue", "red", "purple"))
  print("done plotting")
  return()
}

#########################################################
#########################################################
## PARSE LATENCY BENCHMARKS
#########################################################
#########################################################
# COLS: mode, n, nops, delay, t_per_op
# X=n: number of clients writing
# Y=t_per_op: average time per read
print("plotting latency")
d <- read.csv(file="./latency.csv", head=TRUE, sep=",")
print("plotting...")
my_plot(d, "Read Latency with N clients", "Number of Clients", "ns")
print("done plotting latency")
#########################################################
#########################################################
## PARSE RECOVERY BENCHMARKS
#########################################################
#########################################################
# COLS: mode, n, nops, t_per_op
# X=n: number of operations between reads
# Y=t_per_op: average time of this "first" read
print("reading recovery")
d <- read.csv(file="./recovery.csv", head=TRUE, sep=",")
print("plotting recovery")
print(d)
my_plot(d, "Recovery Time after N Operations", "Number of Operations", "ns")
#########################################################
#########################################################
## PARSE INTEGRATION BENCHMARKS
#########################################################
#########################################################

dev.off()
# 
# # COLS: mode, n, nops, delay, t_per_op
# print("reading integration")
# d <- read.csv(file="./integration.csv", head=TRUE, sep=",")
# 
# for (my_n in unique(d[,2], incomparables = FALSE)) {
#   
#   with_n <- subset(d, n == my_n, 
#                     select=c(mode, n, w, t_per_op))
#   #qprint(with_n)
#   xs <- unique(with_n[,3], incomparables = FALSE) 
#   mode0 <- subset(with_n, mode == 0, select=c(t_per_op))
#   mode1 <- subset(with_n, mode == 1, select=c(t_per_op))
#   mode2 <- subset(with_n, mode == 2, select=c(t_per_op))
#   mode3 <- subset(with_n, mode == 3, select=c(t_per_op))
#   title <- paste(c("n =", my_n), collapse = " ")
#   
#   plot(xs, mode0[,1], col='green', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='w', ylab='ns/op', sub='subtitle', main=title)
#   par(new=T)
#   plot(xs, mode1[,1], col='blue', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
#   par(new=T)
#   plot(xs, mode2[,1], col='red', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
#   par(new=T)
#   plot(xs, mode3[,1], col='black', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
# 
#   legend(max(xs)-100, 1000000000-1000, 
#          c("No-VM", "Enc-No-VM", "VM", "Enc-VM"),
#          text.width = 30,
#          lty=c(1,1,1,1),
#          lwd=c(2.5,2.5),col=c("green", "blue", "red", "black"))
#
# }
#
# for (my_w in unique(d[,3], incomparables = FALSE)) {
#   
#   with_w <- subset(d, w == my_w, 
#                    select=c(mode, n, w, t_per_op))
#   print(with_w)
#   xs <- unique(with_w[,2], incomparables = FALSE) 
#   mode0 <- subset(with_w, mode == 0, select=c(t_per_op))
#   mode1 <- subset(with_w, mode == 1, select=c(t_per_op))
#   mode2 <- subset(with_w, mode == 2, select=c(t_per_op))
#   mode3 <- subset(with_n, mode == 3, select=c(t_per_op))
#   title <- paste(c("w =", my_w), collapse = " ")
#   
#   plot(xs, mode0[,1], col='green', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='n', ylab='ns/op', sub='subtitle', main=title)
#   par(new=T)
#   plot(xs, mode1[,1], col='blue', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
#   par(new=T)
#   plot(xs, mode2[,1], col='red', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
#   par(new=T)
#   plot(xs, mode3[,1], col='black', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
#   legend(max(xs)-20, 1000000000, 
#          c("No-VM", "Enc-No-VM", "VM", "Enc-VM"),
#          text.width = 10,
#          lty=c(1,1,1,1),
#          lwd=c(2.5,2.5),col=c("green", "blue", "red", "black"))
# }
# 
# dev.off()
