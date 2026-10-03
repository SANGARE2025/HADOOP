-- Resultats batch (charges depuis batch-churn/) : clients et solde par pays x tranche d'age
CREATE TABLE IF NOT EXISTS batch_pays_tranche (
    pays        TEXT NOT NULL,
    tranche     TEXT NOT NULL,           -- <30, 30-39, 40-49, 50-59, 60+
    nb_clients  BIGINT NOT NULL,
    solde_total NUMERIC NOT NULL,
    PRIMARY KEY (pays, tranche)
);

-- Streaming : cumul global (une seule ligne, mise a jour en continu)
CREATE TABLE IF NOT EXISTS streaming_cumule (
    id          SMALLINT PRIMARY KEY DEFAULT 1,
    nb_clients  BIGINT NOT NULL,
    solde_total NUMERIC NOT NULL,
    nb_alertes  BIGINT NOT NULL,
    updated_at  TIMESTAMP NOT NULL,
    CONSTRAINT single_row CHECK (id = 1)
);

-- Streaming : cumul par pays
CREATE TABLE IF NOT EXISTS streaming_pays (
    pays        TEXT PRIMARY KEY,
    nb_clients  BIGINT NOT NULL,
    solde_total NUMERIC NOT NULL,
    score_total NUMERIC NOT NULL,        -- somme des CreditScore (moyenne = score_total / nb_clients)
    updated_at  TIMESTAMP NOT NULL
);

-- Streaming : cumul par tranche d'age
CREATE TABLE IF NOT EXISTS streaming_tranche (
    tranche     TEXT PRIMARY KEY,
    nb_clients  BIGINT NOT NULL,
    solde_total NUMERIC NOT NULL,
    updated_at  TIMESTAMP NOT NULL
);

-- Streaming : top 10 des soldes du micro-batch courant
CREATE TABLE IF NOT EXISTS streaming_top_soldes (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    customer_id  BIGINT NOT NULL,
    pays         TEXT,
    solde        NUMERIC NOT NULL,
    credit_score INTEGER,
    PRIMARY KEY (window_start, customer_id)
);

-- Streaming : alertes (solde > 150000 ou score de credit < 500)
CREATE TABLE IF NOT EXISTS streaming_alertes (
    id           BIGSERIAL PRIMARY KEY,
    event_time   TIMESTAMP NOT NULL,
    customer_id  BIGINT NOT NULL,
    pays         TEXT,
    type         TEXT NOT NULL,          -- SOLDE_ELEVE | SCORE_FAIBLE
    solde        NUMERIC NOT NULL,
    credit_score INTEGER
);
