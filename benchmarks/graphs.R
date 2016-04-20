d <- read.csv(file="./rand.csv", head=TRUE, sep=",")
# summary(d)
names(d)

pdf('rplot.pdf')
for (my_n in unique(d[,2], incomparables = FALSE)) {
  
  with_n <- subset(d, n == my_n, 
                    select=c(mode, n, w, t_per_op))
  #qprint(with_n)
  xs <- unique(with_n[,3], incomparables = FALSE) 
  mode0 <- subset(with_n, mode == 0, select=c(t_per_op))
  mode1 <- subset(with_n, mode == 1, select=c(t_per_op))
  mode2 <- subset(with_n, mode == 2, select=c(t_per_op))
  mode3 <- subset(with_n, mode == 3, select=c(t_per_op))
  title <- paste(c("n =", my_n), collapse = " ")
  
  plot(xs, mode0[,1], col='green', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='w', ylab='ns/op', sub='subtitle', main=title)
  par(new=T)
  plot(xs, mode1[,1], col='blue', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
  par(new=T)
  plot(xs, mode2[,1], col='red', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
  par(new=T)
  plot(xs, mode3[,1], col='black', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)

  legend(max(xs)-100, 1000000000-1000, 
         c("No-VM", "Enc-No-VM", "VM", "Enc-VM"),
         text.width = 30,
         lty=c(1,1,1,1),
         lwd=c(2.5,2.5),col=c("green", "blue", "red", "black"))
  
}

for (my_w in unique(d[,3], incomparables = FALSE)) {
  
  with_w <- subset(d, w == my_w, 
                   select=c(mode, n, w, t_per_op))
  print(with_w)
  xs <- unique(with_w[,2], incomparables = FALSE) 
  mode0 <- subset(with_w, mode == 0, select=c(t_per_op))
  mode1 <- subset(with_w, mode == 1, select=c(t_per_op))
  mode2 <- subset(with_w, mode == 2, select=c(t_per_op))
  mode3 <- subset(with_n, mode == 3, select=c(t_per_op))
  title <- paste(c("w =", my_w), collapse = " ")
  
  plot(xs, mode0[,1], col='green', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='n', ylab='ns/op', sub='subtitle', main=title)
  par(new=T)
  plot(xs, mode1[,1], col='blue', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
  par(new=T)
  plot(xs, mode2[,1], col='red', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
  par(new=T)
  plot(xs, mode3[,1], col='black', type='l', xlim=c(0.0, max(xs)), ylim=c(0.0, 1000000000), xlab='', ylab='', axes=F)
  legend(max(xs)-20, 1000000000, 
         c("No-VM", "Enc-No-VM", "VM", "Enc-VM"),
         text.width = 10,
         lty=c(1,1,1,1),
         lwd=c(2.5,2.5),col=c("green", "blue", "red", "black"))
}

dev.off()
