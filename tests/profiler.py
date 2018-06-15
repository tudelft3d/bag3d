import cProfile
import pstats
from bag3d import app

def view_stats(fil):
    stats = pstats.Stats(fil)
    stats.strip_dirs()
    sorted_stats = stats.sort_stats('tottime')
    sorted_stats.print_stats()

def runner():
    app.main(["bag3d", "/home/bdukai/software/bag3d/bag3d_config.yml",
                    "--run-3dfier", "--no-exec"])

if __name__ == '__main__':
    filename = 'profile_output_new'
    cProfile.run('runner()', filename)
    view_stats(filename)
