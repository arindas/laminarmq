# Testing

You may run tests with `cargo` as you would for any other crate. However, since `laminarmq` is
poised to support multiple runtimes, some of them might require some additional setup before running
the steps.

For instance, the `glommio` async runtime which requires an updated linux kernel (at least 5.8) with
`io_uring` support. `glommio` also requires at least 512 KiB of locked memory for `io_uring` to
work. (Note: 512 KiB is the minimum needed to spawn a single executor. Spawning multiple executors
may require you to raise the limit accordingly. I recommend 8192 KiB on a 8 GiB RAM machine.)

First, check the current `memlock` limit:

```sh
ulimit -l

# 512 ## sample output
```

If the `memlock` resource limit (rlimit) is lesser than 512 KiB, you can increase it as follows:
```sh
sudo vi /etc/security/limits.conf
*    hard    memlock        512
*    soft    memlock        512
```

To make the new limits effective, you need to log in to the machine again. Verify whether the limits
have been reflected with `ulimit` as described above.

>(On old WSL versions, you might need to spawn a login shell every time for the limits to be
>reflected:
>```sh
>su ${USER} -l
>```
>The limits persist once inside the login shell. This is not necessary on the latest WSL2 version as
>of 22.12.2022)

Finally, clone the repository and run the tests:
```sh
git clone https://github.com/arindas/laminarmq.git
cd laminarmq/
cargo test
```
