# Betty

This repo contains a proof-of-concept for a light(er)-weight log based on tile storage.
It's intended to serve as a toy to play around with ideas and shapes.

It contains a two exectuables:

- `cmd/leafcreator` which is an in-process leaf-generator which writes directly to storage.
- `cmd/bettyfe` which is a simple HTTP API with a single `/add` POST endpoint.

These executables are used to exercise the other code, and enable experimentation with library/storage ideas and implementations.

## Storage

Currently, there is only a simple POSIX file based storage implemented.
It uses roughly the same layout as `github.com/transparency-dev/serverless-log`, the primary differences being that:

- it supports the concept of `entry bundles` - rather than storing individual entries in separate files, they can be bundled up
  together in a fashion analogous to the way tiles may store multiple nodes.
- multiple concurrent writers is supported, with critical sections enforced with POSIX atomic operations and advisory locks.

## Example use

```bash
❯ go run ./cmd/leafcreator -num_writers=1000 --leaves_per_second=400 -batch_size=256
I0219 12:00:58.860374  139881 main.go:122] CP size 2725616 (+68096); Latency: [Mean: 7.185466ms Min: 0s Max 7.185466ms]
I0219 12:00:59.860746  139881 main.go:122] CP size 2793968 (+68352); Latency: [Mean: 9.208909ms Min: 0s Max 11.232353ms]
I0219 12:01:00.860894  139881 main.go:122] CP size 2861808 (+67840); Latency: [Mean: 10.31317ms Min: 0s Max 12.521693ms]
I0219 12:01:01.861012  139881 main.go:122] CP size 2930416 (+68608); Latency: [Mean: 10.281267ms Min: 0s Max 12.521693ms]
I0219 12:01:02.861204  139881 main.go:122] CP size 2999024 (+68608); Latency: [Mean: 10.613016ms Min: 0s Max 12.521693ms]
I0219 12:01:03.867220  139881 main.go:122] CP size 3046384 (+47360); Latency: [Mean: 12.129849ms Min: 0s Max 21.230848ms]
I0219 12:01:04.867413  139881 main.go:122] CP size 3105008 (+58624); Latency: [Mean: 12.044977ms Min: 0s Max 21.230848ms]
I0219 12:01:05.868108  139881 main.go:122] CP size 3156720 (+51712); Latency: [Mean: 11.826032ms Min: 0s Max 21.230848ms]
I0219 12:01:06.869057  139881 main.go:122] CP size 3225328 (+68608); Latency: [Mean: 12.126077ms Min: 0s Max 21.230848ms]
I0219 12:01:07.869929  139881 main.go:122] CP size 3293680 (+68352); Latency: [Mean: 11.802307ms Min: 0s Max 21.230848ms]
I0219 12:01:08.870718  139881 main.go:122] CP size 3365360 (+71680); Latency: [Mean: 11.822406ms Min: 0s Max 21.230848ms]
I0219 12:01:09.870785  139881 main.go:122] CP size 3434224 (+68864); Latency: [Mean: 11.730493ms Min: 0s Max 21.230848ms]
I0219 12:01:10.870910  139881 main.go:122] CP size 3506416 (+72192); Latency: [Mean: 11.394172ms Min: 0s Max 21.230848ms]
I0219 12:01:11.871170  139881 main.go:122] CP size 3574768 (+68352); Latency: [Mean: 11.300386ms Min: 0s Max 21.230848ms]
I0219 12:01:12.871537  139881 main.go:122] CP size 3646704 (+71936); Latency: [Mean: 11.264518ms Min: 0s Max 21.230848ms]
I0219 12:01:13.871614  139881 main.go:122] CP size 3718640 (+71936); Latency: [Mean: 11.207897ms Min: 0s Max 21.230848ms]
I0219 12:01:14.871675  139881 main.go:122] CP size 3790576 (+71936); Latency: [Mean: 11.189992ms Min: 0s Max 21.230848ms]
I0219 12:01:15.872858  139881 main.go:122] CP size 3858416 (+67840); Latency: [Mean: 11.266441ms Min: 0s Max 21.230848ms]
I0219 12:01:16.873549  139881 main.go:122] CP size 3909104 (+50688); Latency: [Mean: 11.904961ms Min: 0s Max 24.036837ms]
I0219 12:01:17.878228  139881 main.go:122] CP size 3948528 (+39424); Latency: [Mean: 12.833835ms Min: 0s Max 31.41132ms]
I0219 12:01:18.878451  139881 main.go:122] CP size 3989232 (+40704); Latency: [Mean: 12.829395ms Min: 0s Max 31.41132ms]
I0219 12:01:19.879250  139881 main.go:122] CP size 4061168 (+71936); Latency: [Mean: 12.758651ms Min: 0s Max 31.41132ms]
I0219 12:01:20.879347  139881 main.go:122] CP size 4132336 (+71168); Latency: [Mean: 12.626079ms Min: 0s Max 31.41132ms]
I0219 12:01:21.879518  139881 main.go:122] CP size 4204016 (+71680); Latency: [Mean: 12.500682ms Min: 0s Max 31.41132ms]
I0219 12:01:22.963053  139881 main.go:122] CP size 4266480 (+62464); Latency: [Mean: 20.65721ms Min: 0s Max 224.570407ms]
I0219 12:01:23.963478  139881 main.go:122] CP size 4322520 (+56040); Latency: [Mean: 24.256076ms Min: 0s Max 224.570407ms]
I0219 12:01:24.963809  139881 main.go:122] CP size 4386520 (+64000); Latency: [Mean: 23.85558ms Min: 0s Max 224.570407ms]
I0219 12:01:25.963875  139881 main.go:122] CP size 4437976 (+51456); Latency: [Mean: 23.477955ms Min: 0s Max 224.570407ms]
I0219 12:01:26.963957  139881 main.go:122] CP size 4494296 (+56320); Latency: [Mean: 22.965541ms Min: 0s Max 224.570407ms]
I0219 12:01:27.964390  139881 main.go:122] CP size 4547776 (+53480); Latency: [Mean: 22.485064ms Min: 0s Max 224.570407ms]
```

It's fine to run multiple concurrent instances too:

```bash
❯ go run ./cmd/leafcreator -num_writers=1000 --leaves_per_second=400 -batch_size=256
I0219 12:02:08.214855  140182 main.go:122] CP size 4698792 (+71168); Latency: [Mean: 10.842655ms Min: 0s Max 10.842655ms]
I0219 12:02:09.215124  140182 main.go:122] CP size 4769192 (+70400); Latency: [Mean: 10.307859ms Min: 0s Max 10.842655ms]
I0219 12:02:10.215809  140182 main.go:122] CP size 4842664 (+73472); Latency: [Mean: 10.550156ms Min: 0s Max 11.03475ms]
I0219 12:02:11.216324  140182 main.go:122] CP size 4915880 (+73216); Latency: [Mean: 14.770556ms Min: 0s Max 27.431757ms]
I0219 12:02:12.216509  140182 main.go:122] CP size 4992168 (+76288); Latency: [Mean: 19.517224ms Min: 0s Max 38.503896ms]
I0219 12:02:13.216949  140182 main.go:122] CP size 5067688 (+75520); Latency: [Mean: 19.727992ms Min: 0s Max 38.503896ms]
I0219 12:02:14.217769  140182 main.go:122] CP size 5134504 (+66816); Latency: [Mean: 20.029125ms Min: 0s Max 38.503896ms]
I0219 12:02:15.218510  140182 main.go:122] CP size 5205672 (+71168); Latency: [Mean: 19.959601ms Min: 0s Max 38.503896ms]
I0219 12:02:16.219304  140182 main.go:122] CP size 5277096 (+71424); Latency: [Mean: 20.648969ms Min: 0s Max 38.503896ms]
I0219 12:02:17.219526  140182 main.go:122] CP size 5346472 (+69376); Latency: [Mean: 20.981357ms Min: 0s Max 38.503896ms]
I0219 12:02:18.219800  140182 main.go:122] CP size 5421224 (+74752); Latency: [Mean: 21.294084ms Min: 0s Max 38.503896ms]
I0219 12:02:19.220571  140182 main.go:122] CP size 5495208 (+73984); Latency: [Mean: 21.294084ms Min: 0s Max 38.503896ms]
I0219 12:02:20.220842  140182 main.go:122] CP size 5569448 (+74240); Latency: [Mean: 22.102441ms Min: 0s Max 38.503896ms]
I0219 12:02:21.220913  140182 main.go:122] CP size 5639848 (+70400); Latency: [Mean: 21.860446ms Min: 0s Max 38.503896ms]
I0219 12:02:22.221093  140182 main.go:122] CP size 5714856 (+75008); Latency: [Mean: 21.630429ms Min: 0s Max 38.503896ms]
```

and

```bash
❯ go run ./cmd/leafcreator -num_writers=1000 --leaves_per_second=400 -batch_size=256
I0219 12:02:11.292272  140314 main.go:122] CP size 4922280 (+73984); Latency: [Mean: 18.074741ms Min: 0s Max 18.074741ms]
I0219 12:02:12.292390  140314 main.go:122] CP size 4998312 (+76032); Latency: [Mean: 16.836672ms Min: 0s Max 18.074741ms]
I0219 12:02:13.302774  140314 main.go:122] CP size 5068968 (+70656); Latency: [Mean: 17.147329ms Min: 0s Max 18.074741ms]
I0219 12:02:14.303304  140314 main.go:122] CP size 5141160 (+72192); Latency: [Mean: 32.568769ms Min: 0s Max 90.037635ms]
I0219 12:02:15.303793  140314 main.go:122] CP size 5212072 (+70912); Latency: [Mean: 30.132465ms Min: 0s Max 90.037635ms]
I0219 12:02:16.304331  140314 main.go:122] CP size 5283496 (+71424); Latency: [Mean: 28.287203ms Min: 0s Max 90.037635ms]
I0219 12:02:17.304755  140314 main.go:122] CP size 5352872 (+69376); Latency: [Mean: 28.077748ms Min: 0s Max 90.037635ms]
I0219 12:02:18.305179  140314 main.go:122] CP size 5427368 (+74496); Latency: [Mean: 27.752888ms Min: 0s Max 90.037635ms]
I0219 12:02:19.305821  140314 main.go:122] CP size 5501352 (+73984); Latency: [Mean: 27.629597ms Min: 0s Max 90.037635ms]
I0219 12:02:20.306075  140314 main.go:122] CP size 5574824 (+73472); Latency: [Mean: 27.063169ms Min: 0s Max 90.037635ms]
I0219 12:02:21.306417  140314 main.go:122] CP size 5646760 (+71936); Latency: [Mean: 26.353703ms Min: 0s Max 90.037635ms]
I0219 12:02:22.306533  140314 main.go:122] CP size 5721512 (+74752); Latency: [Mean: 26.154184ms Min: 0s Max 90.037635ms]
I0219 12:02:23.307126  140314 main.go:122] CP size 5794728 (+73216); Latency: [Mean: 25.95194ms Min: 0s Max 90.037635ms]
```
