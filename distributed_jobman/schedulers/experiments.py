from distributed_jobman.database import database


REQUIRED_CLUSTER_OPTIONS = ["root", "max_running", "max_queued"]
DEFAULT_CLUSTER_VALUES = dict(duree="null", mem="null", env="null")


def _validate_clusters_for_saving(clusters, experiment):
    valid_clusters = dict()
    for name, cluster in clusters.iteritems():
        missing_keys = [key for key in REQUIRED_CLUSTER_OPTIONS
                        if key not in cluster]
        if len(missing_keys) > 0:
            raise ValueError("Required options are missing for cluster %s: %s" %
                             (name, str(missing_keys)))

        missing_keys = [key for key in DEFAULT_CLUSTER_VALUES.keys()
                        if key not in experiment and key not in cluster]
        if len(missing_keys) > 0:
            raise ValueError("Options are defined neither for cluster %s or "
                             "default experiment values: %s" %
                             (name, str(missing_keys)))

        valid_clusters[name] = DEFAULT_CLUSTER_VALUES.copy()
        valid_clusters[name].update(cluster)

    return valid_clusters


def _validate_clusters_for_loading(clusters, experiment):
    valid_clusters = dict()
    for name, cluster in clusters.iteritems():

        valid_clusters[name] = cluster.copy()

        for key, value in cluster.iteritems():
            if value == "null":
                cluster[key] = experiment[key]

        valid_clusters[name].update(cluster)

    return valid_clusters


def _filter_cluster(cluster, experiments):
    if cluster is None:
        return experiments

    return filter(lambda exp: cluster in exp["clusters"], experiments)


def load_experiments(cluster=None, filter_eq_dct=None):

    experiments = _filter_cluster(
        cluster, database.load("experiments", filter_eq_dct=filter_eq_dct))

    valid_experiments = []
    for experiment in experiments:
        valid_experiments.append(experiment)
        valid_experiments[-1]["clusters"] = _validate_clusters_for_loading(
            experiment["clusters"], experiment)

    return valid_experiments


def save_experiment(name, table_name, clusters, duree, mem, env, gpu):

    already_existing = load_experiments(filter_eq_dct=dict(name=name))

    if len(already_existing) > 0:
        experiment = already_existing[0]
        update_dict = dict([(key, value)
                            for key, value in locals().iteritems()
                            if key in experiment])
        update_dict["clusters"] = _validate_clusters_for_saving(
            clusters, experiment)
        database.update("experiments", [experiment], update_dict)
    else:
        experiment = dict(
            name=name,
            table=table_name,
            duree=duree,
            mem=mem,
            env=env,
            gpu=gpu)

        experiment["clusters"] = _validate_clusters_for_saving(
            clusters, experiment)

        experiment = database.save("experiments", experiment)

    return experiment


def update_experiments(experiments, update_dict):
    return database.update("experiments", experiments, update_dict)


def delete_experiments(experiments):
    return database.delete("experiments", experiments)
