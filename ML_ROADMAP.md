# Roadmap : Module d'Estimation Immobilière (Machine Learning ELK)

Ce document décrit la procédure pas-à-pas pour transformer les données DVF indexées dans Elasticsearch en un moteur d'estimation de prix (Régression via Kibana ML).

## 📋 Prérequis
- [x] Stack ELK (Elasticsearch, Kibana) fonctionnelle.
- [x] Données DVF chargées dans un index (ex: `gov-dvf` ou `gov-dvf-paris`).
- [ ] Licence : La fonctionnalité Machine Learning nécessite une licence (Basic/Trial ou Gold+). En local/dev, l'activation de la "Trial" de 30 jours est souvent nécessaire via *Stack Management > License Management*.

---

## 📅 Phase 1 : Vérification des Données (Data Audit)
Avant d'entraîner le modèle, nous devons confirmer que les "ingrédients" sont bons.

**Action :** Exécuter dans Kibana **Dev Tools** :
```json
GET gov-dvf/_mapping
```

**Checklist des champs indispensables :**
- [ ] `valeur_fonciere` (Type: `float` ou `double`) -> **Cible (Ce qu'on veut prédire)**
- [ ] `surface_reelle_bati` (Type: `integer` ou `float`)
- [ ] `nombre_pieces_principales` (Type: `integer`)
- [ ] `latitude` (Type: `float`) 
- [ ] `longitude` (Type: `float`)
- [ ] `type_local` (Type: `keyword` ou `text`)

> 💡 **Note :** Si `latitude` et `longitude` sont uniquement dans un objet `geo_point` (ex: `pin.location`), le ML peut les utiliser mais c'est souvent plus simple pour une régression d'avoir les champs à plat si on veut voir leur poids individuel. Cependant, Kibana gère de mieux en mieux les Geo-types.

---

## 🧠 Phase 2 : Création du Modèle (Training)

**Outil :** Kibana > Analytics > Machine Learning > Data Frame Analytics.

### Étape 2.1 : Configuration du Job
1.  Cliquer sur **Create job**.
2.  Sélectionner **Regression**.
3.  **Source index** : Choisir `gov-dvf` (ou votre index filtré `gov-dvf-paris`).
4.  **Job ID** : `estimateur_prix_immo_v1`.

### Étape 2.2 : Paramètres d'apprentissage
5.  **Dependent variable** (La question) : Sélectionner `valeur_fonciere`.
6.  **Included fields** (Les critères) :
    *   *Décochez "All" pour éviter le bruit (dates, IDs, adresses textes...)*
    *   ✅ `surface_reelle_bati`
    *   ✅ `nombre_pieces_principales`
    *   ✅ `latitude`
    *   ✅ `longitude`
    *   ✅ `type_local`
    *   *(Optionnel) `code_postal`*
7.  **Training percent** : Laisser à `80` (80% entrainement, 20% test).

### Étape 2.3 : Lancement
8.  Cliquer sur **Create**.
9.  Cliquer sur **Start now**.
10. Attendre que le statut passe à **Stopped** (Progression : 100%).

---

## 🎯 Phase 3 : Estimation (Inférence)
Une fois le modèle entraîné, il est stocké dans Elasticsearch et prêt à répondre.

### Méthode "Manuelle" (Via Dev Tools)
Pour estimer un bien spécifique ("J'ai un appart de 3 pièces..."), utilisez la commande suivante :

```json
POST _ml/trained_models/estimateur_prix_immo_v1*/deployment/_infer
{
  "docs": [
    {
      "surface_reelle_bati": 65,      
      "nombre_pieces_principales": 3,
      "type_local": "Appartement",
      "latitude": 48.8566,
      "longitude": 2.3522
    }
  ]
}
```

### Méthode "Industrielle" (Intégration)
Pour intégrer cela dans votre application ou dashboard :
1.  Créer un **Ingest Pipeline** qui utilise ce processeur d'inférence.
2.  Ou appeler l'API Elasticsearch ci-dessus depuis votre code Python/API.

---

## 📊 Phase 4 : Analyse des Performances (Optionnel)
Pour vérifier si le modèle est fiable :
1.  Aller sur la liste des jobs Data Frame Analytics.
2.  Cliquer sur **View details** > **Evaluation**.
3.  Regarder le **Generalization error** (plus c'est bas, mieux c'est).
4.  Regarder l'**Importance des features** : Vous verrez probablement que la `surface` est le critère n°1, suivi de la localisation (`latitude`/`longitude`).
