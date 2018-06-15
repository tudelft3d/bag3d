from line_profiler import LineProfiler
from bag3d import app
from bag3d.batch3dfier import process

args = ["bag3d", "/home/bdukai/software/bag3d/bag3d_config.yml",
                    "--run-3dfier", "--no-exec"]
lp = LineProfiler()
lp.add_function(process.run)
lp_wrapper = lp(app.main)
lp_wrapper(args)
lp.print_stats()