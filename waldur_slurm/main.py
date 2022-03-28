from . import waldur_rest_client, slurm_cluster_client


def app():
    print(waldur_rest_client)
    print(slurm_cluster_client)

if __name__ == "__main__":
    app()
